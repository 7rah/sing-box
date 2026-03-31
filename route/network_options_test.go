package route

import (
	"context"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func TestNewNetworkManagerDefaultBindInterfaces(t *testing.T) {
	tests := []struct {
		name    string
		options option.RouteOptions
		wantV4  string
		wantV6  string
	}{
		{
			name: "legacy default interface",
			options: option.RouteOptions{
				DefaultInterface: "en0",
			},
			wantV4: "en0",
			wantV6: "",
		},
		{
			name: "ipv6 only default bind interface",
			options: option.RouteOptions{
				DefaultIPv6BindInterface: "en8",
			},
			wantV4: "",
			wantV6: "en8",
		},
		{
			name: "ipv6 bind with auto detect strategy",
			options: option.RouteOptions{
				AutoDetectInterface:      true,
				DefaultIPv6BindInterface: "en8",
				DefaultNetworkStrategy:   common.Ptr(option.NetworkStrategy(C.NetworkStrategyDefault)),
			},
			wantV4: "",
			wantV6: "en8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewNetworkManager(context.Background(), logger.NOP(), tt.options, option.DNSOptions{})
			require.NoError(t, err)

			defaultOptions := manager.DefaultOptions()
			require.Equal(t, tt.wantV4, defaultOptions.BindInterface)
			require.Equal(t, tt.wantV6, defaultOptions.BindIPv6Interface)
		})
	}
}
