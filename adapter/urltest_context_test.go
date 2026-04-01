package adapter

import (
	"context"
	"testing"
)

func TestURLTestContextMarker(t *testing.T) {
	ctx := context.Background()
	if IsURLTestFromContext(ctx) {
		t.Fatalf("expected false")
	}
	ctx = ContextWithURLTest(ctx)
	if !IsURLTestFromContext(ctx) {
		t.Fatalf("expected true")
	}
}
