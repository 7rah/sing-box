package v2raygrpclite

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

type clientConn interface {
	RoundTrip(*http.Request) (*http.Response, error)
	Close() error
}

type clientConnFactory func(ctx context.Context) (clientConn, error)

type poolConfig struct {
	maxConnections     int
	maxStreams         int
	maxConnecting      int
	maxReuse           int
	maxAge             time.Duration
	waitTimeout        time.Duration
	minConnections     int
	failureBackoff     time.Duration
	maxFailureBackoff  time.Duration
	restoreStableAfter time.Duration
}

type poolState uint8

const (
	poolStateNormal poolState = iota
	poolStateCooldown
	poolStateHalfOpen
)

func (s poolState) String() string {
	switch s {
	case poolStateCooldown:
		return "cooldown"
	case poolStateHalfOpen:
		return "half_open"
	default:
		return "normal"
	}
}

type pool struct {
	id      int64
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     poolConfig
	factory clientConnFactory

	reqCh chan any
	done  chan struct{}

	nextWaiterID atomic.Int64
	nextConnID   atomic.Int64
}

var nextPoolID atomic.Int64

type poolConn struct {
	id        int64
	conn      clientConn
	created   time.Time
	inUse     int
	used      int
	retiring  bool
	probation bool
	failed    bool
}

type leaseConn struct {
	inner     clientConn
	broken    *atomic.Bool
	brokenErr *atomic.Value
}

func (c *leaseConn) markBroken(err error) {
	if err == nil {
		return
	}
	c.broken.Store(true)
	if c.brokenErr != nil {
		c.brokenErr.Store(err)
	}
}

func (c *leaseConn) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.inner.RoundTrip(r)
	if shouldMarkRoundTripBroken(err) {
		c.markBroken(err)
	}
	return resp, err
}

func (c *leaseConn) Close() error {
	c.broken.Store(true)
	return c.inner.Close()
}

type acquireReq struct {
	id    int64
	ctx   context.Context
	resCh chan<- acquireRes
}

type acquireRes struct {
	conn    clientConn
	release func()
	err     error
}

type cancelReq struct {
	id int64
}

type releaseReq struct {
	connID int64
	broken bool
	cause  error
}

type dialResult struct {
	conn clientConn
	err  error
}

type waiter struct {
	id    int64
	ctx   context.Context
	resCh chan<- acquireRes
}

func newPool(ctx context.Context, cfg poolConfig, factory clientConnFactory) *pool {
	if cfg.maxStreams <= 0 {
		cfg.maxStreams = 1
	}
	if cfg.maxConnecting <= 0 {
		cfg.maxConnecting = 1
	}
	if cfg.failureBackoff <= 0 {
		cfg.failureBackoff = time.Second
	}
	if cfg.maxFailureBackoff <= 0 {
		cfg.maxFailureBackoff = 16 * time.Second
	}
	if cfg.restoreStableAfter <= 0 {
		cfg.restoreStableAfter = 3 * time.Minute
	}

	poolCtx, cancel := context.WithCancel(ctx)
	p := &pool{
		id:      nextPoolID.Add(1),
		ctx:     poolCtx,
		cancel:  cancel,
		cfg:     cfg,
		factory: factory,
		reqCh:   make(chan any, 128),
		done:    make(chan struct{}),
	}
	go p.loop()
	log.TraceContext(
		p.ctx,
		"v2raygrpclite/pool#", p.id,
		" start max_conns=", p.cfg.maxConnections,
		" min_conns=", p.cfg.minConnections,
		" max_streams=", p.cfg.maxStreams,
		" max_connecting=", p.cfg.maxConnecting,
		" max_reuse=", p.cfg.maxReuse,
		" max_age=", p.cfg.maxAge,
		" wait_timeout=", p.cfg.waitTimeout,
		" failure_backoff=", p.cfg.failureBackoff,
		" max_failure_backoff=", p.cfg.maxFailureBackoff,
		" restore_stable_after=", p.cfg.restoreStableAfter,
	)
	return p
}

func (p *pool) Acquire(ctx context.Context) (clientConn, func(), error) {
	if p.cfg.waitTimeout > 0 {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > p.cfg.waitTimeout {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.cfg.waitTimeout)
			defer cancel()
		}
	}

	id := p.nextWaiterID.Add(1)
	resCh := make(chan acquireRes, 1)
	req := acquireReq{ //nolint:exhaustruct
		id:    id,
		ctx:   ctx,
		resCh: resCh,
	}

	select {
	case p.reqCh <- req:
	case <-p.ctx.Done():
		log.TraceContext(ctx, "v2raygrpclite/pool#", p.id, " acquire enqueue failed waiter#", id, " err=", context.Canceled)
		return nil, nil, context.Canceled
	}

	select {
	case res := <-resCh:
		if res.err != nil {
			log.TraceContext(ctx, "v2raygrpclite/pool#", p.id, " acquire waiter#", id, " done err=", res.err)
		}
		return res.conn, res.release, res.err
	case <-ctx.Done():
		log.TraceContext(ctx, "v2raygrpclite/pool#", p.id, " acquire waiter#", id, " canceled err=", ctx.Err())
		select {
		case p.reqCh <- cancelReq{id: id}:
		case <-p.ctx.Done():
		}
		select {
		case res := <-resCh:
			if res.release != nil {
				res.release()
			}
		default:
		}
		return nil, nil, ctx.Err()
	}
}

func (p *pool) Close() error {
	p.cancel()
	<-p.done
	log.TraceContext(context.Background(), "v2raygrpclite/pool#", p.id, " closed")
	return nil
}

func (p *pool) loop() {
	defer close(p.done)

	var conns []*poolConn
	var waiters []*waiter
	connsByID := make(map[int64]*poolConn)
	var pendingConnecting int
	var demanded bool
	var pickIndex int

	state := poolStateNormal
	var consecutiveFailures int
	var lastFailureErr error
	var lastFailureAt time.Time
	var nextDialAt time.Time

	var stateTimer *time.Timer
	var stateTimerCh <-chan time.Time
	var stateTimerAt time.Time

	clearTimer := func() {
		if stateTimer == nil {
			stateTimerCh = nil
			stateTimerAt = time.Time{}
			return
		}
		if !stateTimer.Stop() {
			select {
			case <-stateTimer.C:
			default:
			}
		}
		stateTimerCh = nil
		stateTimerAt = time.Time{}
	}

	scheduleTimer := func(at time.Time) {
		if at.IsZero() {
			clearTimer()
			return
		}
		if stateTimerAt.Equal(at) {
			return
		}
		delay := time.Until(at)
		if delay < 0 {
			delay = 0
		}
		if stateTimer == nil {
			stateTimer = time.NewTimer(delay)
		} else {
			if !stateTimer.Stop() {
				select {
				case <-stateTimer.C:
				default:
				}
			}
			stateTimer.Reset(delay)
		}
		stateTimerCh = stateTimer.C
		stateTimerAt = at
	}

	updateTimer := func() {
		switch state {
		case poolStateCooldown:
			scheduleTimer(nextDialAt)
		case poolStateHalfOpen:
			scheduleTimer(lastFailureAt.Add(p.cfg.restoreStableAfter))
		default:
			clearTimer()
		}
	}

	closeAll := func() {
		clearTimer()
		log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " shutdown conns=", len(conns), " pending_connecting=", pendingConnecting, " waiters=", len(waiters))
		for _, c := range conns {
			_ = c.conn.Close()
		}
		conns = nil
		waiters = nil
		for k := range connsByID {
			delete(connsByID, k)
		}
	}

	refresh := func(now time.Time) {
		if len(conns) == 0 {
			return
		}

		dst := conns[:0]
		for _, c := range conns {
			if p.cfg.maxReuse > 0 && c.used >= p.cfg.maxReuse {
				c.retiring = true
			}
			if p.cfg.maxAge > 0 && now.Sub(c.created) >= p.cfg.maxAge {
				c.retiring = true
			}
			if c.retiring && c.inUse == 0 {
				log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " conn#", c.id, " retire close used=", c.used, " age=", now.Sub(c.created), " in_use=", c.inUse)
				_ = c.conn.Close()
				delete(connsByID, c.id)
				continue
			}
			dst = append(dst, c)
		}
		conns = dst
		if pickIndex >= len(conns) {
			pickIndex = 0
		}
	}

	connMaxStreams := func(c *poolConn) int {
		if c.probation {
			return 1
		}
		return p.cfg.maxStreams
	}

	selectHalfOpenConn := func() *poolConn {
		var keep *poolConn
		for _, c := range conns {
			if c.retiring {
				continue
			}
			if keep == nil ||
				(keep.probation && !c.probation) ||
				(keep.probation == c.probation && c.inUse > keep.inUse) {
				keep = c
			}
		}
		return keep
	}

	enforceHalfOpen := func(now time.Time) {
		if state != poolStateHalfOpen {
			return
		}
		keep := selectHalfOpenConn()
		for _, c := range conns {
			if c.retiring {
				continue
			}
			if keep != nil && c.id == keep.id {
				continue
			}
			c.retiring = true
		}
		refresh(now)
	}

	restoreNormal := func() {
		log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " state ", state, " -> ", poolStateNormal)
		state = poolStateNormal
		consecutiveFailures = 0
		lastFailureErr = nil
		lastFailureAt = time.Time{}
		nextDialAt = time.Time{}
		updateTimer()
	}

	enterHalfOpen := func(now time.Time) {
		log.TraceContext(
			p.ctx,
			"v2raygrpclite/pool#", p.id,
			" state ", state, " -> ", poolStateHalfOpen,
			" stable_window=", p.cfg.restoreStableAfter,
		)
		state = poolStateHalfOpen
		nextDialAt = time.Time{}
		enforceHalfOpen(now)
		updateTimer()
	}

	backoffDuration := func() time.Duration {
		backoff := p.cfg.failureBackoff
		for i := 1; i < consecutiveFailures; i++ {
			if backoff >= p.cfg.maxFailureBackoff {
				return p.cfg.maxFailureBackoff
			}
			if backoff > p.cfg.maxFailureBackoff/2 {
				return p.cfg.maxFailureBackoff
			}
			backoff *= 2
		}
		if backoff > p.cfg.maxFailureBackoff {
			return p.cfg.maxFailureBackoff
		}
		return backoff
	}

	enterCooldown := func(now time.Time, err error) {
		consecutiveFailures++
		lastFailureAt = now
		lastFailureErr = err
		backoff := backoffDuration()
		nextDialAt = now.Add(backoff)
		log.TraceContext(
			p.ctx,
			"v2raygrpclite/pool#", p.id,
			" state ", state, " -> ", poolStateCooldown,
			" failures=", consecutiveFailures,
			" backoff=", backoff,
			" next_dial_at=", nextDialAt,
			" err=", err,
		)
		state = poolStateCooldown
		updateTimer()
	}

	cooldownError := func() error {
		if lastFailureErr != nil {
			return E.Cause(lastFailureErr, "v2ray-grpc: pool reconnect cooldown")
		}
		return E.New("v2ray-grpc: pool reconnect cooldown")
	}

	advanceState := func(now time.Time) {
		for {
			if state == poolStateCooldown && !nextDialAt.IsZero() && !now.Before(nextDialAt) {
				enterHalfOpen(now)
				continue
			}
			if state == poolStateHalfOpen {
				restoreAt := lastFailureAt.Add(p.cfg.restoreStableAfter)
				if !restoreAt.IsZero() && !now.Before(restoreAt) {
					restoreNormal()
					continue
				}
				enforceHalfOpen(now)
			}
			break
		}
		updateTimer()
	}

	activeConnections := func() int {
		active := 0
		for _, c := range conns {
			if !c.retiring {
				active++
			}
		}
		return active
	}

	effectiveMaxConnections := func() int {
		if state == poolStateHalfOpen {
			return 1
		}
		return p.cfg.maxConnections
	}

	effectiveMinConnections := func() int {
		if !demanded {
			return 0
		}
		if state == poolStateHalfOpen {
			return 1
		}
		return p.cfg.minConnections
	}

	availableConn := func(now time.Time) *poolConn {
		refresh(now)
		if len(conns) == 0 {
			return nil
		}
		if pickIndex >= len(conns) {
			pickIndex = 0
		}

		minInUse := int(^uint(0) >> 1)
		for _, c := range conns {
			if c.retiring || c.inUse >= connMaxStreams(c) {
				continue
			}
			if c.inUse < minInUse {
				minInUse = c.inUse
			}
		}
		if minInUse == int(^uint(0)>>1) {
			return nil
		}

		for i := 0; i < len(conns); i++ {
			index := (pickIndex + i) % len(conns)
			c := conns[index]
			if c.retiring || c.inUse >= connMaxStreams(c) {
				continue
			}
			if c.inUse == minInUse {
				pickIndex = (index + 1) % len(conns)
				return c
			}
		}
		return nil
	}

	sendAcquire := func(w *waiter, c *poolConn) {
		c.inUse++
		c.used++
		if p.cfg.maxReuse > 0 && c.used >= p.cfg.maxReuse {
			c.retiring = true
		}
		log.TraceContext(
			w.ctx,
			"v2raygrpclite/pool#", p.id,
			" acquire waiter#", w.id,
			" -> conn#", c.id,
			" in_use=", c.inUse, "/", connMaxStreams(c),
			" used=", c.used,
			" retiring=", c.retiring,
			" probation=", c.probation,
			" state=", state,
		)
		var broken atomic.Bool
		var brokenErr atomic.Value
		var once sync.Once
		release := func() {
			once.Do(func() {
				var cause error
				if loaded := brokenErr.Load(); loaded != nil {
					cause = loaded.(error)
				}
				select {
				case p.reqCh <- releaseReq{connID: c.id, broken: broken.Load(), cause: cause}:
				case <-p.ctx.Done():
				}
			})
		}
		w.resCh <- acquireRes{
			conn:    &leaseConn{inner: c.conn, broken: &broken, brokenErr: &brokenErr},
			release: release,
		}
	}

	fulfillWaiters := func(now time.Time) {
		if len(waiters) == 0 {
			return
		}
		dst := waiters[:0]
		for _, w := range waiters {
			if w.ctx.Err() != nil {
				continue
			}
			c := availableConn(now)
			if c == nil {
				dst = append(dst, w)
				continue
			}
			sendAcquire(w, c)
		}
		waiters = dst
	}

	failWaiters := func(err error) {
		if len(waiters) > 0 {
			log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " fail waiters=", len(waiters), " err=", err)
		}
		for _, w := range waiters {
			w.resCh <- acquireRes{err: err} //nolint:exhaustruct
		}
		waiters = nil
	}

	startDial := func(reason string) bool {
		if state == poolStateCooldown {
			log.TraceContext(
				p.ctx,
				"v2raygrpclite/pool#", p.id,
				" dial skip reason=", reason,
				" (cooldown) next_dial_at=", nextDialAt,
			)
			return false
		}
		maxConnections := effectiveMaxConnections()
		active := activeConnections()
		if maxConnections > 0 && active+pendingConnecting >= maxConnections {
			log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial skip reason=", reason, " (max_connections) active=", active, " pending_connecting=", pendingConnecting, " state=", state)
			return false
		}
		if pendingConnecting >= p.cfg.maxConnecting {
			log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial skip reason=", reason, " (max_connecting) conns=", len(conns), " pending_connecting=", pendingConnecting)
			return false
		}
		pendingConnecting++
		log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial start reason=", reason, " pending_connecting=", pendingConnecting, " conns=", len(conns), " waiters=", len(waiters), " state=", state)
		go func() {
			conn, err := p.factory(p.ctx)
			if err != nil && conn != nil {
				_ = conn.Close()
				conn = nil
			}
			select {
			case p.reqCh <- dialResult{conn: conn, err: err}:
			case <-p.ctx.Done():
				if conn != nil {
					_ = conn.Close()
				}
			}
		}()
		return true
	}

	ensureBaseline := func(now time.Time) {
		target := effectiveMinConnections()
		if target <= 0 {
			return
		}
		refresh(now)
		enforceHalfOpen(now)
		for activeConnections()+pendingConnecting < target {
			if !startDial("baseline") {
				break
			}
		}
	}

	ensureCapacity := func(now time.Time) {
		refresh(now)
		enforceHalfOpen(now)

		if len(waiters) == 0 {
			return
		}
		if state == poolStateCooldown {
			return
		}

		freeSlots := 0
		for _, c := range conns {
			if c.retiring {
				continue
			}
			if maxStreams := connMaxStreams(c); c.inUse < maxStreams {
				freeSlots += maxStreams - c.inUse
			}
		}

		potentialSlots := freeSlots + pendingConnecting
		for potentialSlots < len(waiters) {
			if !startDial("capacity") {
				break
			}
			potentialSlots++
		}
	}

	for {
		select {
		case <-p.ctx.Done():
			failWaiters(context.Canceled)
			closeAll()
			return

		case <-stateTimerCh:
			now := time.Now()
			advanceState(now)
			fulfillWaiters(now)
			ensureCapacity(now)
			ensureBaseline(now)

		case msg := <-p.reqCh:
			now := time.Now()
			advanceState(now)
			switch m := msg.(type) {
			case acquireReq:
				if !demanded && p.cfg.minConnections > 0 {
					demanded = true
					log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " baseline demand start min_conns=", p.cfg.minConnections)
				}
				if m.ctx.Err() != nil {
					m.resCh <- acquireRes{err: m.ctx.Err()} //nolint:exhaustruct
					break
				}
				if c := availableConn(now); c != nil {
					sendAcquire(&waiter{id: m.id, ctx: m.ctx, resCh: m.resCh}, c) //nolint:exhaustruct
					ensureBaseline(now)
					break
				}
				if state == poolStateCooldown {
					m.resCh <- acquireRes{err: cooldownError()} //nolint:exhaustruct
					break
				}
				waiters = append(waiters, &waiter{id: m.id, ctx: m.ctx, resCh: m.resCh}) //nolint:exhaustruct
				log.TraceContext(m.ctx, "v2raygrpclite/pool#", p.id, " acquire queued waiter#", m.id, " waiters=", len(waiters), " state=", state)
				ensureCapacity(now)
				ensureBaseline(now)

			case cancelReq:
				if len(waiters) == 0 {
					break
				}
				log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " acquire cancel waiter#", m.id, " waiters=", len(waiters))
				dst := waiters[:0]
				for _, w := range waiters {
					if w.id == m.id {
						continue
					}
					dst = append(dst, w)
				}
				waiters = dst

			case releaseReq:
				c := connsByID[m.connID]
				if c == nil {
					log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " release conn#", m.connID, " ignored (missing)")
					break
				}
				if m.broken {
					if !c.failed {
						c.failed = true
						if m.cause == nil {
							m.cause = E.New("v2ray-grpc: pooled connection became unhealthy")
						}
						enterCooldown(now, m.cause)
						failWaiters(cooldownError())
					}
					c.retiring = true
					_ = c.conn.Close()
				} else if c.probation {
					c.probation = false
				}
				if c.inUse > 0 {
					c.inUse--
				}
				log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " release conn#", c.id, " broken=", m.broken, " in_use=", c.inUse, "/", connMaxStreams(c), " used=", c.used, " age=", now.Sub(c.created).Truncate(time.Second), " retiring=", c.retiring, " probation=", c.probation, " waiters=", len(waiters), " state=", state)
				advanceState(now)
				fulfillWaiters(now)
				ensureCapacity(now)
				ensureBaseline(now)

			case dialResult:
				if pendingConnecting > 0 {
					pendingConnecting--
				}
				if m.err != nil {
					log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial failed pending_connecting=", pendingConnecting, " err=", m.err)
					if m.conn != nil {
						_ = m.conn.Close()
					}
					enterCooldown(now, m.err)
					failWaiters(cooldownError())
					break
				}
				if m.conn == nil {
					log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial canceled pending_connecting=", pendingConnecting)
					failWaiters(context.Canceled)
					break
				}
				c := &poolConn{ //nolint:exhaustruct
					id:        p.nextConnID.Add(1),
					conn:      m.conn,
					created:   now,
					probation: true,
				}
				conns = append(conns, c)
				connsByID[c.id] = c
				log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial ok conn#", c.id, " conns=", len(conns), " pending_connecting=", pendingConnecting, " waiters=", len(waiters), " probation=", c.probation, " state=", state)
				enforceHalfOpen(now)
				fulfillWaiters(now)
				ensureCapacity(now)
				ensureBaseline(now)
			}
		}
	}
}
