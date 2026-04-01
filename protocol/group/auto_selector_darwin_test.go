//go:build darwin

package group

import (
	"testing"

	"golang.org/x/net/route"

	"github.com/stretchr/testify/require"
)

func TestMatchDarwinInterfaceMessage(t *testing.T) {
	require.True(t, matchDarwinInterfaceMessage(&route.InterfaceMessage{Name: "en8"}, "en8", 23))
	require.False(t, matchDarwinInterfaceMessage(&route.InterfaceMessage{Name: "en9"}, "en8", 23))
	require.True(t, matchDarwinInterfaceMessage(&route.InterfaceAddrMessage{Index: 23}, "en8", 23))
	require.False(t, matchDarwinInterfaceMessage(&route.InterfaceAddrMessage{Index: 24}, "en8", 23))
	require.False(t, matchDarwinInterfaceMessage(&route.InterfaceAddrMessage{Index: 23}, "en8", 0))
	require.False(t, matchDarwinInterfaceMessage(&route.RouteMessage{}, "en8", 23))
}
