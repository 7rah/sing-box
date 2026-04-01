package v2raygrpclite

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/log"
)

type clientConn interface {
	RoundTrip(*http.Request) (*http.Response, error)
	Close() error
}

type clientConnFactory func(ctx context.Context) (clientConn, error)

type poolConfig struct {
	maxConnections int
	maxStreams     int
	maxConnecting  int
	maxReuse       int
	maxAge         time.Duration
	waitTimeout    time.Duration
	minConnections int
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
	id       int64
	conn     clientConn
	created  time.Time
	inUse    int
	used     int
	retiring bool
}

type leaseConn struct {
	inner  clientConn
	broken *atomic.Bool
}

func (c *leaseConn) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.inner.RoundTrip(r)
	if err != nil {
		c.broken.Store(true)
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
	log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " start max_conns=", p.cfg.maxConnections, " min_conns=", p.cfg.minConnections, " max_streams=", p.cfg.maxStreams, " max_connecting=", p.cfg.maxConnecting, " max_reuse=", p.cfg.maxReuse, " max_age=", p.cfg.maxAge, " wait_timeout=", p.cfg.waitTimeout)
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
	log.TraceContext(ctx, "v2raygrpclite/pool#", p.id, " acquire enqueue waiter#", id)
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
		} else {
			log.TraceContext(ctx, "v2raygrpclite/pool#", p.id, " acquire waiter#", id, " done ok")
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

	closeAll := func() {
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
			if c.retiring || c.inUse >= p.cfg.maxStreams {
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
			if c.retiring || c.inUse >= p.cfg.maxStreams {
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
		log.TraceContext(w.ctx, "v2raygrpclite/pool#", p.id, " acquire waiter#", w.id, " -> conn#", c.id, " in_use=", c.inUse, "/", p.cfg.maxStreams, " used=", c.used, " retiring=", c.retiring)
		var broken atomic.Bool
		var once sync.Once
		release := func() {
			once.Do(func() {
				select {
				case p.reqCh <- releaseReq{connID: c.id, broken: broken.Load()}:
				case <-p.ctx.Done():
				}
			})
		}
		w.resCh <- acquireRes{conn: &leaseConn{inner: c.conn, broken: &broken}, release: release} //nolint:exhaustruct
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
		active := 0
		for _, c := range conns {
			if !c.retiring {
				active++
			}
		}
		if p.cfg.maxConnections > 0 && active+pendingConnecting >= p.cfg.maxConnections {
			log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial skip reason=", reason, " (max_connections) active=", active, " pending_connecting=", pendingConnecting)
			return false
		}
		if pendingConnecting >= p.cfg.maxConnecting {
			log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial skip reason=", reason, " (max_connecting) conns=", len(conns), " pending_connecting=", pendingConnecting)
			return false
		}
		pendingConnecting++
		log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial start reason=", reason, " pending_connecting=", pendingConnecting, " conns=", len(conns), " waiters=", len(waiters))
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
		if !demanded || p.cfg.minConnections <= 0 {
			return
		}
		refresh(now)
		active := 0
		for _, c := range conns {
			if !c.retiring {
				active++
			}
		}
		target := p.cfg.minConnections
		for active+pendingConnecting < target {
			if !startDial("baseline") {
				break
			}
		}
	}

	ensureCapacity := func(now time.Time) {
		refresh(now)

		if len(waiters) == 0 {
			return
		}

		freeSlots := 0
		for _, c := range conns {
			if c.retiring {
				continue
			}
			if c.inUse < p.cfg.maxStreams {
				freeSlots += p.cfg.maxStreams - c.inUse
			}
		}

		potentialSlots := freeSlots + pendingConnecting*p.cfg.maxStreams
		for potentialSlots < len(waiters) {
			if !startDial("capacity") {
				break
			}
			potentialSlots += p.cfg.maxStreams
		}
	}

	for {
		select {
		case <-p.ctx.Done():
			failWaiters(context.Canceled)
			closeAll()
			return

		case msg := <-p.reqCh:
			now := time.Now()
			switch m := msg.(type) {
			case acquireReq:
				log.TraceContext(m.ctx, "v2raygrpclite/pool#", p.id, " acquire recv waiter#", m.id, " conns=", len(conns), " pending_connecting=", pendingConnecting, " waiters=", len(waiters))
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
				waiters = append(waiters, &waiter{id: m.id, ctx: m.ctx, resCh: m.resCh}) //nolint:exhaustruct
				log.TraceContext(m.ctx, "v2raygrpclite/pool#", p.id, " acquire queued waiter#", m.id, " waiters=", len(waiters))
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
					c.retiring = true
					_ = c.conn.Close()
				}
				if c.inUse > 0 {
					c.inUse--
				}
				log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " release conn#", c.id, " broken=", m.broken, " in_use=", c.inUse, "/", p.cfg.maxStreams, " used=", c.used, " retiring=", c.retiring, " waiters=", len(waiters))
				fulfillWaiters(now)
				ensureCapacity(now)
				ensureBaseline(now)

			case dialResult:
				pendingConnecting--
				if m.err != nil {
					log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial failed pending_connecting=", pendingConnecting, " err=", m.err)
					if m.conn != nil {
						_ = m.conn.Close()
					}
					failWaiters(m.err)
					break
				}
				if m.conn == nil {
					log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial canceled pending_connecting=", pendingConnecting)
					failWaiters(context.Canceled)
					break
				}
				c := &poolConn{ //nolint:exhaustruct
					id:      p.nextConnID.Add(1),
					conn:    m.conn,
					created: now,
				}
				conns = append(conns, c)
				connsByID[c.id] = c
				log.TraceContext(p.ctx, "v2raygrpclite/pool#", p.id, " dial ok conn#", c.id, " conns=", len(conns), " pending_connecting=", pendingConnecting, " waiters=", len(waiters))
				fulfillWaiters(now)
				ensureCapacity(now)
				ensureBaseline(now)
			}
		}
	}
}
