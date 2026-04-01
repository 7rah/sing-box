package v2raygrpclite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
)

type captureLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *captureLogger) add(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprint(args...))
}

func (l *captureLogger) Trace(args ...any)                             {}
func (l *captureLogger) Debug(args ...any)                             {}
func (l *captureLogger) Info(args ...any)                              {}
func (l *captureLogger) Warn(args ...any)                              {}
func (l *captureLogger) Error(args ...any)                             {}
func (l *captureLogger) Fatal(args ...any)                             {}
func (l *captureLogger) Panic(args ...any)                             {}
func (l *captureLogger) TraceContext(ctx context.Context, args ...any) { l.add(args...) }
func (l *captureLogger) DebugContext(ctx context.Context, args ...any) {}
func (l *captureLogger) InfoContext(ctx context.Context, args ...any)  {}
func (l *captureLogger) WarnContext(ctx context.Context, args ...any)  {}
func (l *captureLogger) ErrorContext(ctx context.Context, args ...any) {}
func (l *captureLogger) FatalContext(ctx context.Context, args ...any) {}
func (l *captureLogger) PanicContext(ctx context.Context, args ...any) {}

func TestPoolEmitsTraceLogs(t *testing.T) {
	prev := log.StdLogger()
	capture := &captureLogger{}
	log.SetStdLogger(capture)
	defer log.SetStdLogger(prev)

	factory := func(ctx context.Context) (clientConn, error) {
		return &fakeClientConn{}, nil
	}

	p := newPool(context.Background(), poolConfig{
		maxConnections: 1,
		maxStreams:     1,
		maxConnecting:  1,
		waitTimeout:    50 * time.Millisecond,
	}, factory)
	defer p.Close()

	_, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	release()

	capture.mu.Lock()
	all := strings.Join(capture.entries, "\n")
	capture.mu.Unlock()

	if !strings.Contains(all, "v2raygrpclite/pool") {
		t.Fatalf("expected pool trace logs, got: %s", all)
	}
}
