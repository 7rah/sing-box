package dialer

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

func TestResolveDefaultBindPlan(t *testing.T) {
	tests := []struct {
		name               string
		disableDefaultBind bool
		autoDetect         bool
		hasPlatform        bool
		defaults           adapter.NetworkOptions
		wantMode4          bindMode
		wantMode6          bindMode
		wantInterface4     string
		wantInterface6     string
		wantStrategy       bool
	}{
		{
			name:               "outbound explicit bind disables route defaults",
			disableDefaultBind: true,
			autoDetect:         true,
			hasPlatform:        true,
			defaults: adapter.NetworkOptions{
				BindInterface:     "en0",
				BindIPv6Interface: "en8",
			},
			wantMode4:    bindModeNone,
			wantMode6:    bindModeNone,
			wantStrategy: false,
		},
		{
			name: "default interface applies to both families",
			defaults: adapter.NetworkOptions{
				BindInterface: "en0",
			},
			wantMode4:      bindModeExplicit,
			wantMode6:      bindModeExplicit,
			wantInterface4: "en0",
			wantInterface6: "en0",
		},
		{
			name: "ipv6 only default bind interface",
			defaults: adapter.NetworkOptions{
				BindIPv6Interface: "en8",
			},
			wantMode4:      bindModeNone,
			wantMode6:      bindModeExplicit,
			wantInterface6: "en8",
		},
		{
			name:       "ipv6 bind with auto detect",
			autoDetect: true,
			defaults: adapter.NetworkOptions{
				BindIPv6Interface: "en8",
			},
			wantMode4:      bindModeAutoDetect,
			wantMode6:      bindModeExplicit,
			wantInterface6: "en8",
		},
		{
			name: "split families",
			defaults: adapter.NetworkOptions{
				BindInterface:     "en0",
				BindIPv6Interface: "en8",
			},
			wantMode4:      bindModeExplicit,
			wantMode6:      bindModeExplicit,
			wantInterface4: "en0",
			wantInterface6: "en8",
		},
		{
			name:        "platform auto detect keeps strategy for unresolved families",
			autoDetect:  true,
			hasPlatform: true,
			defaults: adapter.NetworkOptions{
				BindIPv6Interface: "en8",
			},
			wantMode4:      bindModeProtect,
			wantMode6:      bindModeExplicit,
			wantInterface6: "en8",
			wantStrategy:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := resolveDefaultBindPlan(tt.disableDefaultBind, tt.autoDetect, tt.hasPlatform, tt.defaults)
			require.Equal(t, tt.wantMode4, plan.mode4)
			require.Equal(t, tt.wantMode6, plan.mode6)
			require.Equal(t, tt.wantInterface4, plan.interface4)
			require.Equal(t, tt.wantInterface6, plan.interface6)
			require.Equal(t, tt.wantStrategy, plan.enableStrategy)
		})
	}
}
