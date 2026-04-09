package v2raygrpclite

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"

	"golang.org/x/net/http2"
)

func TestTransportForContextRoundRobin(t *testing.T) {
	transports := []*http2.Transport{{}, {}, {}}
	client := &Client{
		transports: transports,
	}
	for i, expected := range []int{0, 1, 2, 0, 1} {
		if actual := transportIndex(transports, client.transportForContext(context.Background())); actual != expected {
			t.Fatalf("round-robin pick %d: got %d, want %d", i, actual, expected)
		}
	}
}

func TestTransportForContextPinnedDestination(t *testing.T) {
	transports := []*http2.Transport{{}, {}, {}}
	client := &Client{
		transports: transports,
		pinnedDestinations: map[string]struct{}{
			"www.gstatic.com:443": {},
		},
	}
	ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("www.gstatic.com", 443),
	})
	for i := 0; i < 3; i++ {
		if actual := transportIndex(transports, client.transportForContext(ctx)); actual != 0 {
			t.Fatalf("pinned pick %d: got %d, want 0", i, actual)
		}
	}
	if actual := transportIndex(transports, client.transportForContext(context.Background())); actual != 0 {
		t.Fatalf("round-robin should still start from slot 0 after pinned picks, got %d", actual)
	}
}

func transportIndex(transports []*http2.Transport, target *http2.Transport) int {
	for i, transport := range transports {
		if transport == target {
			return i
		}
	}
	return -1
}
