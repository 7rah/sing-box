package v2raygrpclite

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

const (
	testFailureBackoff     = 10 * time.Millisecond
	testMaxFailureBackoff  = 40 * time.Millisecond
	testRestoreStableAfter = 80 * time.Millisecond
)

type fakeClientConn struct {
	closed atomic.Bool
}

func (c *fakeClientConn) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *fakeClientConn) Close() error {
	c.closed.Store(true)
	return nil
}

type scriptedClientConn struct {
	roundTripErr error
	closed       atomic.Bool
}

func (c *scriptedClientConn) RoundTrip(*http.Request) (*http.Response, error) {
	if c.roundTripErr != nil {
		return nil, c.roundTripErr
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func (c *scriptedClientConn) Close() error {
	c.closed.Store(true)
	return nil
}

type countedClientConn struct {
	failOn map[int]error
	calls  atomic.Int32
	closed atomic.Bool
}

func (c *countedClientConn) RoundTrip(*http.Request) (*http.Response, error) {
	call := int(c.calls.Add(1))
	if err := c.failOn[call]; err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func (c *countedClientConn) Close() error {
	c.closed.Store(true)
	return nil
}

func TestPoolBrokenConnIsNotReusedAndRedials(t *testing.T) {
	var dials atomic.Int32
	var first atomic.Bool
	first.Store(true)

	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		if first.Swap(false) {
			return &scriptedClientConn{roundTripErr: errors.New("boom")}, nil
		}
		return &scriptedClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         1,
		maxConnecting:      1,
		waitTimeout:        200 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for attempts := 0; ; attempts++ {
		c, release, err := p.Acquire(context.Background())
		if err != nil {
			if attempts >= 10 {
				t.Fatalf("Acquire error after retries: %v", err)
			}
			select {
			case <-deadline.C:
				t.Fatalf("timed out waiting for recovery (dials=%d): last error=%v", dials.Load(), err)
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}
		resp, err := c.RoundTrip(&http.Request{})
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		release()
		if err == nil {
			break
		}
		if attempts >= 5 {
			t.Fatalf("did not recover after %d attempts (dials=%d): last error=%v", attempts+1, dials.Load(), err)
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for recovery (dials=%d): last error=%v", dials.Load(), err)
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got, want := int(dials.Load()), 2; got != want {
		t.Fatalf("dial count: got %d, want %d", got, want)
	}
}

func TestPoolBaselineRedialsAfterBrokenConn(t *testing.T) {
	var dials atomic.Int32
	var first atomic.Bool
	first.Store(true)

	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		if first.Swap(false) {
			return &scriptedClientConn{roundTripErr: errors.New("boom")}, nil
		}
		return &scriptedClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         1,
		maxConnecting:      1,
		minConnections:     1,
		waitTimeout:        time.Second,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	c, release, err := p.Acquire(context.Background()) // triggers demanded=true
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	_, _ = c.RoundTrip(&http.Request{})
	release()

	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for dials.Load() < 2 {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for baseline redial (dials=%d)", dials.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestPoolDialsEnoughConnectionsForConcurrency(t *testing.T) {
	var dials atomic.Int32
	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     3,
		maxStreams:         2,
		maxConnecting:      3,
		waitTimeout:        time.Second,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	startWarm := make(chan struct{})
	holdWarm := make(chan struct{})
	var warmed atomic.Int32

	var warmWG sync.WaitGroup
	warmWG.Add(3)
	for range 3 {
		go func() {
			defer warmWG.Done()
			<-startWarm
			_, release, err := p.Acquire(context.Background())
			if err != nil {
				t.Errorf("warm Acquire error: %v", err)
				return
			}
			warmed.Add(1)
			<-holdWarm
			release()
		}()
	}
	close(startWarm)

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for warmed.Load() != 3 {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for warm acquires (%d/3)", warmed.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(holdWarm)
	warmWG.Wait()

	start := make(chan struct{})
	hold := make(chan struct{})
	var acquired atomic.Int32

	var wg sync.WaitGroup
	wg.Add(5)
	for range 5 {
		go func() {
			defer wg.Done()
			<-start
			_, release, err := p.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire error: %v", err)
				return
			}
			acquired.Add(1)
			<-hold
			release()
		}()
	}
	close(start)

	deadline.Reset(time.Second)
	for acquired.Load() != 5 {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for acquires (%d/5)", acquired.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got, want := int(dials.Load()), 3; got != want {
		t.Fatalf("dial count: got %d, want %d", got, want)
	}

	close(hold)
	wg.Wait()
}

func TestPoolAcquireWaitTimeout(t *testing.T) {
	factory := func(ctx context.Context) (clientConn, error) {
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         1,
		maxConnecting:      1,
		waitTimeout:        50 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	_, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(1) error: %v", err)
	}
	defer release()

	start := time.Now()
	_, _, err = p.Acquire(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire(2) error: got %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("Acquire(2) returned too quickly: %v", elapsed)
	}
}

func TestPoolDialFailureUnblocksWaiters(t *testing.T) {
	var shouldFail atomic.Bool
	shouldFail.Store(true)
	factory := func(ctx context.Context) (clientConn, error) {
		if shouldFail.Load() {
			return nil, errors.New("dial failed")
		}
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         1,
		maxConnecting:      1,
		waitTimeout:        200 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	done := make(chan error, 1)
	go func() {
		_, _, err := p.Acquire(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected dial error")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Acquire did not return (possible deadlock)")
	}

	shouldFail.Store(false)
	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for {
		_, release, err := p.Acquire(context.Background())
		if err == nil {
			release()
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("Acquire after recovery error: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestPoolCloseUnblocksWaiters(t *testing.T) {
	factory := func(ctx context.Context) (clientConn, error) {
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         1,
		maxConnecting:      1,
		waitTimeout:        time.Second,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)

	_, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(1) error: %v", err)
	}
	defer release()

	done := make(chan error, 1)
	go func() {
		_, _, err := p.Acquire(context.Background())
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	_ = p.Close()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire(2) error: got %v, want %v", err, context.Canceled)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Acquire(2) did not return after Close()")
	}
}

type failingPool struct{}

func (f failingPool) Acquire(ctx context.Context) (clientConn, func(), error) {
	return nil, func() {}, errors.New("pool should not be used")
}

func (f failingPool) Close() error { return nil }

type stubTransport struct {
	called atomic.Int32
}

func (s *stubTransport) DialContext(ctx context.Context) (net.Conn, error) {
	s.called.Add(1)
	return nil, errors.New("stub dial")
}

func (s *stubTransport) Close() error { return nil }

func TestPooledClientBypassesPoolForURLTest(t *testing.T) {
	base := &stubTransport{}
	client := &pooledClient{
		base: base,
		pool: failingPool{},
	}

	ctx := adapter.ContextWithURLTest(context.Background())
	_, _ = client.DialContext(ctx)

	if got := base.called.Load(); got != 1 {
		t.Fatalf("base DialContext called %d times, want 1", got)
	}
}

func TestPoolMinConnectionsPrewarmAfterDemand(t *testing.T) {
	var dials atomic.Int32
	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxStreams:         128,
		maxConnecting:      2,
		minConnections:     2,
		waitTimeout:        time.Second,
		maxConnections:     0,
		maxReuse:           0,
		maxAge:             0,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	time.Sleep(20 * time.Millisecond)
	if got := dials.Load(); got != 0 {
		t.Fatalf("unexpected pre-dial before demand: %d", got)
	}

	_, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	defer release()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for dials.Load() < 2 {
		select {
		case <-deadline.C:
			t.Fatalf("expected min_connections prewarm to reach 2, got %d", dials.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestPoolProbationAllowsSingleStreamUntilFirstCleanRelease(t *testing.T) {
	var dials atomic.Int32
	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		return &scriptedClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         4,
		maxConnecting:      1,
		waitTimeout:        40 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	_, release1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(1) error: %v", err)
	}

	start := time.Now()
	_, _, err = p.Acquire(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire(2) error: got %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("Acquire(2) returned too quickly: %v", elapsed)
	}

	release1()

	hold := make(chan struct{})
	var acquired atomic.Int32
	var wg sync.WaitGroup
	wg.Add(4)
	for range 4 {
		go func() {
			defer wg.Done()
			_, release, err := p.Acquire(context.Background())
			if err != nil {
				t.Errorf("post-probation Acquire error: %v", err)
				return
			}
			acquired.Add(1)
			<-hold
			release()
		}()
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for acquired.Load() != 4 {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for post-probation acquires (%d/4)", acquired.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got, want := int(dials.Load()), 1; got != want {
		t.Fatalf("dial count: got %d, want %d", got, want)
	}

	close(hold)
	wg.Wait()
}

func TestPoolHalfOpenKeepsSingleConnectionUntilStable(t *testing.T) {
	var dials atomic.Int32
	var first atomic.Bool
	first.Store(true)

	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		if first.Swap(false) {
			return &scriptedClientConn{roundTripErr: errors.New("boom")}, nil
		}
		return &scriptedClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     3,
		maxStreams:         1,
		maxConnecting:      3,
		waitTimeout:        40 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: 200 * time.Millisecond,
	}, factory)
	defer p.Close()

	c, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(1) error: %v", err)
	}
	_, _ = c.RoundTrip(&http.Request{})
	release()

	var heldRelease func()
	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for {
		_, release, err := p.Acquire(context.Background())
		if err != nil {
			select {
			case <-deadline.C:
				t.Fatalf("Acquire during half-open recovery error: %v", err)
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}
		heldRelease = release
		break
	}

	start := time.Now()
	_, _, err = p.Acquire(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire while half-open busy error: got %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("Acquire while half-open busy returned too quickly: %v", elapsed)
	}
	if got, want := int(dials.Load()), 2; got != want {
		t.Fatalf("dial count while half-open: got %d, want %d", got, want)
	}

	heldRelease()
}

func TestPoolHalfOpenRestoresNormalAfterStableWindow(t *testing.T) {
	var dials atomic.Int32
	var first atomic.Bool
	first.Store(true)

	factory := func(ctx context.Context) (clientConn, error) {
		dials.Add(1)
		if first.Swap(false) {
			return &scriptedClientConn{roundTripErr: errors.New("boom")}, nil
		}
		return &scriptedClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     2,
		maxStreams:         1,
		maxConnecting:      2,
		waitTimeout:        120 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: 50 * time.Millisecond,
	}, factory)
	defer p.Close()

	c, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(1) error: %v", err)
	}
	_, _ = c.RoundTrip(&http.Request{})
	release()

	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for {
		c, release, err = p.Acquire(context.Background())
		if err == nil {
			release()
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("Acquire during half-open warmup error: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}

	time.Sleep(70 * time.Millisecond)

	start := make(chan struct{})
	hold := make(chan struct{})
	var acquired atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			<-start
			_, release, err := p.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire after stable restore error: %v", err)
				return
			}
			acquired.Add(1)
			<-hold
			release()
		}()
	}
	close(start)

	deadline.Reset(300 * time.Millisecond)
	for acquired.Load() != 2 {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for restored acquires (%d/2)", acquired.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got, want := int(dials.Load()), 3; got != want {
		t.Fatalf("dial count after stable restore: got %d, want %d", got, want)
	}

	close(hold)
	wg.Wait()
}

func TestPoolHalfOpenFailureResetsStableWindowAndBackoff(t *testing.T) {
	var dials atomic.Int32
	factory := func(ctx context.Context) (clientConn, error) {
		switch dial := int(dials.Add(1)); dial {
		case 1:
			return &countedClientConn{failOn: map[int]error{1: errors.New("boom-1")}}, nil
		case 2:
			return &countedClientConn{failOn: map[int]error{2: errors.New("boom-2")}}, nil
		default:
			return &countedClientConn{}, nil
		}
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         1,
		maxConnecting:      1,
		waitTimeout:        50 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: 120 * time.Millisecond,
	}, factory)
	defer p.Close()

	c, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(1) error: %v", err)
	}
	_, _ = c.RoundTrip(&http.Request{})
	release()

	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for {
		c, release, err = p.Acquire(context.Background())
		if err == nil {
			_, roundTripErr := c.RoundTrip(&http.Request{})
			if roundTripErr != nil {
				t.Fatalf("unexpected half-open warmup RoundTrip error: %v", roundTripErr)
			}
			release()
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("Acquire during half-open warmup error: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}

	time.Sleep(20 * time.Millisecond)

	c, release, err = p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(3) error: %v", err)
	}
	_, err = c.RoundTrip(&http.Request{})
	release()
	if err == nil {
		t.Fatalf("expected second failure")
	}

	time.Sleep(12 * time.Millisecond)
	_, _, err = p.Acquire(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cooldown") {
		t.Fatalf("expected cooldown error after second failure, got: %v", err)
	}
}

func TestPoolBrokenConnectionCountsFailureOnce(t *testing.T) {
	var dials atomic.Int32
	factory := func(ctx context.Context) (clientConn, error) {
		switch dial := int(dials.Add(1)); dial {
		case 1:
			return &countedClientConn{failOn: map[int]error{
				2: errors.New("boom-a"),
				3: errors.New("boom-b"),
			}}, nil
		default:
			return &countedClientConn{}, nil
		}
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections:     1,
		maxStreams:         2,
		maxConnecting:      1,
		waitTimeout:        60 * time.Millisecond,
		failureBackoff:     testFailureBackoff,
		maxFailureBackoff:  testMaxFailureBackoff,
		restoreStableAfter: testRestoreStableAfter,
	}, factory)
	defer p.Close()

	c, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire warmup error: %v", err)
	}
	resp, err := c.RoundTrip(&http.Request{})
	if err != nil {
		t.Fatalf("warmup RoundTrip error: %v", err)
	}
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	release()

	c1, release1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failure stream 1 error: %v", err)
	}
	c2, release2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failure stream 2 error: %v", err)
	}

	_, err1 := c1.RoundTrip(&http.Request{})
	_, err2 := c2.RoundTrip(&http.Request{})
	release1()
	release2()
	if err1 == nil || err2 == nil {
		t.Fatalf("expected both in-flight streams to fail, got err1=%v err2=%v", err1, err2)
	}

	time.Sleep(12 * time.Millisecond)

	deadline := time.NewTimer(300 * time.Millisecond)
	defer deadline.Stop()
	for {
		_, release, err := p.Acquire(context.Background())
		if err == nil {
			release()
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("Acquire after single-count failure error: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
