package v2raygrpclite

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
)

type traceCaptureLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *traceCaptureLogger) Trace(args ...any)                             {}
func (l *traceCaptureLogger) Debug(args ...any)                             {}
func (l *traceCaptureLogger) Info(args ...any)                              {}
func (l *traceCaptureLogger) Warn(args ...any)                              {}
func (l *traceCaptureLogger) Error(args ...any)                             {}
func (l *traceCaptureLogger) Fatal(args ...any)                             {}
func (l *traceCaptureLogger) Panic(args ...any)                             {}
func (l *traceCaptureLogger) TraceContext(ctx context.Context, args ...any) { l.add(args...) }
func (l *traceCaptureLogger) DebugContext(ctx context.Context, args ...any) {}
func (l *traceCaptureLogger) InfoContext(ctx context.Context, args ...any)  {}
func (l *traceCaptureLogger) WarnContext(ctx context.Context, args ...any)  {}
func (l *traceCaptureLogger) ErrorContext(ctx context.Context, args ...any) {}
func (l *traceCaptureLogger) FatalContext(ctx context.Context, args ...any) {}
func (l *traceCaptureLogger) PanicContext(ctx context.Context, args ...any) {}

func (l *traceCaptureLogger) add(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprint(args...))
}

func TestPoolBalancesAcrossConnectionsByLeastInUse(t *testing.T) {
	prev := log.StdLogger()
	capture := &traceCaptureLogger{}
	log.SetStdLogger(capture)
	defer log.SetStdLogger(prev)

	factory := func(ctx context.Context) (clientConn, error) {
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxStreams:     128,
		maxConnecting:  2,
		minConnections: 2,
		waitTimeout:    time.Second,
	}, factory)
	defer p.Close()

	// Trigger demand + baseline; keep the first stream open so in_use matters.
	_, release0, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire(0) error: %v", err)
	}
	defer release0()

	// Wait until both connections are created.
	dialOkRe := regexp.MustCompile(`dial ok conn#(\d+)`)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for 2 connections to be created")
		case <-time.After(5 * time.Millisecond):
			capture.mu.Lock()
			all := append([]string(nil), capture.entries...)
			capture.mu.Unlock()

			unique := make(map[string]bool)
			for _, line := range all {
				if m := dialOkRe.FindStringSubmatch(line); m != nil {
					unique[m[1]] = true
				}
			}
			if len(unique) >= 2 {
				goto baselineReady
			}
		}
	}

baselineReady:
	var releases []func()
	defer func() {
		for _, r := range releases {
			r()
		}
	}()

	for i := 0; i < 6; i++ {
		_, release, err := p.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire(%d) error: %v", i+1, err)
		}
		releases = append(releases, release)
	}

	assignRe := regexp.MustCompile(`acquire waiter#\d+ -> conn#(\d+)`)
	countByConn := make(map[int]int)
	capture.mu.Lock()
	all := append([]string(nil), capture.entries...)
	capture.mu.Unlock()
	for _, line := range all {
		m := assignRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		id, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		countByConn[id]++
	}

	if len(countByConn) < 2 {
		t.Fatalf("expected assignments across >=2 connections, got: %+v", countByConn)
	}

	min := math.MaxInt
	max := 0
	for _, c := range countByConn {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	if max-min > 1 {
		t.Fatalf("expected near-even distribution, got: %+v", countByConn)
	}
}
