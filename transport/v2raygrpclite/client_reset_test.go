package v2raygrpclite

import (
	"context"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
)

type canceledPool struct {
	closeCalled atomic.Bool
}

func (p *canceledPool) Acquire(ctx context.Context) (clientConn, func(), error) {
	return nil, func() {}, context.Canceled
}

func (p *canceledPool) Close() error {
	p.closeCalled.Store(true)
	return nil
}

func TestPooledClientCloseClearsPool(t *testing.T) {
	base := &stubTransport{}
	p := &canceledPool{}
	client := &pooledClient{
		base: base,
		pool: p,
	}

	_ = client.Close()

	if !p.closeCalled.Load() {
		t.Fatalf("expected pool.Close to be called")
	}
	if client.pool != nil {
		t.Fatalf("expected pool to be cleared after Close()")
	}
}

func TestPooledClientDialRecreatesPoolAfterClose(t *testing.T) {
	base := &stubTransport{}
	var created atomic.Int32
	makePool := func() poolAcquirer {
		created.Add(1)
		return &canceledPool{}
	}

	client := &pooledClient{
		base:     base,
		makePool: makePool,
		pool:     nil,
		url:      &url.URL{Scheme: "https", Host: "example.com", Path: "/"},
		host:     "example.com",
	}

	_, _ = client.DialContext(context.Background())

	deadline := time.NewTimer(200 * time.Millisecond)
	defer deadline.Stop()
	for created.Load() == 0 {
		select {
		case <-deadline.C:
			t.Fatalf("expected makePool to be called")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestNewClientRejectsInvalidPoolBackoffConfig(t *testing.T) {
	_, err := NewClient(context.Background(), nil, M.ParseSocksaddrHostPort("example.com", 443), option.V2RayGRPCOptions{
		ServiceName: "TunService",
		Pool: &option.V2RayGRPCPoolOptions{
			Enabled:           true,
			FailureBackoff:    badoption.Duration(2 * time.Second),
			MaxFailureBackoff: badoption.Duration(time.Second),
		},
	}, nil)
	if err == nil {
		t.Fatalf("expected invalid pool config error")
	}
	if !strings.Contains(err.Error(), "max_failure_backoff") {
		t.Fatalf("unexpected error: %v", err)
	}
}
