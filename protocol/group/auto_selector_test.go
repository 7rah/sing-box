package group

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	outboundadapter "github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/control"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestAutoSelectorValidation(t *testing.T) {
	logger := log.NewNOPFactory().Logger()
	_, err := NewAutoSelector(context.Background(), nil, logger, "auto", option.AutoSelectorOutboundOptions{})
	require.ErrorContains(t, err, "missing match")

	_, err = NewAutoSelector(context.Background(), nil, logger, "auto", option.AutoSelectorOutboundOptions{
		Match: "match",
	})
	require.ErrorContains(t, err, "missing fallback")

	_, err = NewAutoSelector(context.Background(), nil, logger, "auto", option.AutoSelectorOutboundOptions{
		Match:    "match",
		Fallback: "fallback",
	})
	require.ErrorContains(t, err, "missing watch_interface")
}

func TestAutoSelectorSelectsMatchAndRechecksOnNetworkUpdate(t *testing.T) {
	autoSelector, networkManager := newAutoSelectorForTest(t, []string{"en8"}, map[string]adapter.Outbound{
		"proxy-v6": newMockOutbound("proxy-v6"),
		"proxy-v4": newMockOutbound("proxy-v4"),
	})
	require.NoError(t, autoSelector.Start())
	t.Cleanup(func() {
		require.NoError(t, autoSelector.Close())
	})

	require.Equal(t, "proxy-v6", autoSelector.Now())

	networkManager.setInterfaces()
	networkManager.monitor.Emit()

	require.Eventually(t, func() bool {
		return autoSelector.Now() == "proxy-v4"
	}, time.Second, 10*time.Millisecond)

	networkManager.setInterfaces("en8")
	networkManager.monitor.Emit()

	require.Eventually(t, func() bool {
		return autoSelector.Now() == "proxy-v6"
	}, time.Second, 10*time.Millisecond)
}

func TestAutoSelectorManualOverrideClearsWhenAutoSelectionChanges(t *testing.T) {
	autoSelector, networkManager := newAutoSelectorForTest(t, []string{"en8"}, map[string]adapter.Outbound{
		"proxy-v6": newMockOutbound("proxy-v6"),
		"proxy-v4": newMockOutbound("proxy-v4"),
	})
	require.NoError(t, autoSelector.Start())
	t.Cleanup(func() {
		require.NoError(t, autoSelector.Close())
	})

	require.True(t, autoSelector.SelectOutbound("proxy-v4"))
	require.Equal(t, "proxy-v4", autoSelector.Now())

	networkManager.monitor.Emit()
	require.Eventually(t, func() bool {
		return autoSelector.Now() == "proxy-v4"
	}, time.Second, 10*time.Millisecond)

	autoSelector.stateAccess.Lock()
	require.Equal(t, "proxy-v4", autoSelector.manualOverrideTag)
	autoSelector.stateAccess.Unlock()

	networkManager.setInterfaces()
	networkManager.monitor.Emit()

	require.Eventually(t, func() bool {
		autoSelector.stateAccess.Lock()
		defer autoSelector.stateAccess.Unlock()
		return autoSelector.Now() == "proxy-v4" && autoSelector.manualOverrideTag == ""
	}, time.Second, 10*time.Millisecond)

	networkManager.setInterfaces("en8")
	networkManager.monitor.Emit()

	require.Eventually(t, func() bool {
		return autoSelector.Now() == "proxy-v6"
	}, time.Second, 10*time.Millisecond)
}

func TestAutoSelectorStartFailsWhenOutboundMissing(t *testing.T) {
	autoSelector, _ := newAutoSelectorForTest(t, []string{"en8"}, map[string]adapter.Outbound{
		"proxy-v6": newMockOutbound("proxy-v6"),
	})
	require.ErrorContains(t, autoSelector.Start(), "outbound 1 not found")
}

func newAutoSelectorForTest(t *testing.T, interfaces []string, outbounds map[string]adapter.Outbound) (*AutoSelector, *mockNetworkManager) {
	t.Helper()
	oldWatcherConstructor := autoSelectorPlatformWatcherConstructor
	autoSelectorPlatformWatcherConstructor = func(selector *AutoSelector) (io.Closer, error) {
		return nil, nil
	}
	t.Cleanup(func() {
		autoSelectorPlatformWatcherConstructor = oldWatcherConstructor
	})

	networkManager := newMockNetworkManager(interfaces...)
	outboundManager := &mockOutboundManager{outbounds: outbounds}
	ctx := context.Background()
	ctx = service.ContextWith[adapter.NetworkManager](ctx, networkManager)
	ctx = service.ContextWith[adapter.OutboundManager](ctx, outboundManager)
	ctx = service.ContextWith[adapter.ConnectionManager](ctx, &mockConnectionManager{})

	outbound, err := NewAutoSelector(ctx, nil, log.NewNOPFactory().Logger(), "auto", option.AutoSelectorOutboundOptions{
		Match:          "proxy-v6",
		Fallback:       "proxy-v4",
		WatchInterface: "en8",
	})
	require.NoError(t, err)
	return outbound.(*AutoSelector), networkManager
}

type mockOutbound struct {
	outboundadapter.Adapter
}

func newMockOutbound(tag string) *mockOutbound {
	return &mockOutbound{
		Adapter: outboundadapter.NewAdapter(C.TypeDirect, tag, []string{N.NetworkTCP, N.NetworkUDP}, nil),
	}
}

func (o *mockOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (o *mockOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not implemented")
}

type mockOutboundManager struct {
	outbounds map[string]adapter.Outbound
}

func (m *mockOutboundManager) Start(stage adapter.StartStage) error { return nil }

func (m *mockOutboundManager) Close() error { return nil }

func (m *mockOutboundManager) Outbounds() []adapter.Outbound {
	var outbounds []adapter.Outbound
	for _, outbound := range m.outbounds {
		outbounds = append(outbounds, outbound)
	}
	return outbounds
}

func (m *mockOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, loaded := m.outbounds[tag]
	return outbound, loaded
}

func (m *mockOutboundManager) Default() adapter.Outbound {
	for _, outbound := range m.outbounds {
		return outbound
	}
	return nil
}

func (m *mockOutboundManager) Remove(tag string) error { return nil }

func (m *mockOutboundManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	return errors.New("not implemented")
}

type mockConnectionManager struct{}

func (m *mockConnectionManager) Start(stage adapter.StartStage) error { return nil }

func (m *mockConnectionManager) Close() error { return nil }

func (m *mockConnectionManager) Count() int { return 0 }

func (m *mockConnectionManager) CloseAll() {}

func (m *mockConnectionManager) TrackConn(conn net.Conn) net.Conn { return conn }

func (m *mockConnectionManager) TrackPacketConn(conn net.PacketConn) net.PacketConn { return conn }

func (m *mockConnectionManager) NewConnection(ctx context.Context, this N.Dialer, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
}

func (m *mockConnectionManager) NewPacketConnection(ctx context.Context, this N.Dialer, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
}

type mockNetworkManager struct {
	finder     *mockInterfaceFinder
	interfaces []control.Interface
	monitor    *mockNetworkUpdateMonitor
}

func newMockNetworkManager(interfaces ...string) *mockNetworkManager {
	manager := &mockNetworkManager{
		finder:  &mockInterfaceFinder{},
		monitor: &mockNetworkUpdateMonitor{},
	}
	manager.setInterfaces(interfaces...)
	return manager
}

func (m *mockNetworkManager) setInterfaces(interfaceNames ...string) {
	interfaces := make([]control.Interface, 0, len(interfaceNames))
	for index, name := range interfaceNames {
		interfaces = append(interfaces, control.Interface{
			Index: index + 1,
			Name:  name,
		})
	}
	m.interfaces = interfaces
}

func (m *mockNetworkManager) Start(stage adapter.StartStage) error { return nil }

func (m *mockNetworkManager) Close() error { return nil }

func (m *mockNetworkManager) Initialize(ruleSets []adapter.RuleSet) {}

func (m *mockNetworkManager) InterfaceFinder() control.InterfaceFinder { return m.finder }

func (m *mockNetworkManager) UpdateInterfaces() error {
	m.finder.SetInterfaces(m.interfaces)
	return nil
}

func (m *mockNetworkManager) DefaultNetworkInterface() *adapter.NetworkInterface { return nil }

func (m *mockNetworkManager) NetworkInterfaces() []adapter.NetworkInterface { return nil }

func (m *mockNetworkManager) AutoDetectInterface() bool { return false }

func (m *mockNetworkManager) AutoDetectInterfaceFunc() control.Func { return nil }

func (m *mockNetworkManager) ProtectFunc() control.Func { return nil }

func (m *mockNetworkManager) DefaultOptions() adapter.NetworkOptions { return adapter.NetworkOptions{} }

func (m *mockNetworkManager) RegisterAutoRedirectOutputMark(mark uint32) error { return nil }

func (m *mockNetworkManager) AutoRedirectOutputMark() uint32 { return 0 }

func (m *mockNetworkManager) AutoRedirectOutputMarkFunc() control.Func { return nil }

func (m *mockNetworkManager) NetworkMonitor() tun.NetworkUpdateMonitor { return m.monitor }

func (m *mockNetworkManager) InterfaceMonitor() tun.DefaultInterfaceMonitor { return nil }

func (m *mockNetworkManager) PackageManager() tun.PackageManager { return nil }

func (m *mockNetworkManager) NeedWIFIState() bool { return false }

func (m *mockNetworkManager) WIFIState() adapter.WIFIState { return adapter.WIFIState{} }

func (m *mockNetworkManager) UpdateWIFIState() {}

func (m *mockNetworkManager) ResetNetwork() {}

type mockInterfaceFinder struct {
	interfaces []control.Interface
}

func (f *mockInterfaceFinder) SetInterfaces(interfaces []control.Interface) {
	f.interfaces = append([]control.Interface(nil), interfaces...)
}

func (f *mockInterfaceFinder) Update() error { return nil }

func (f *mockInterfaceFinder) Interfaces() []control.Interface {
	return append([]control.Interface(nil), f.interfaces...)
}

func (f *mockInterfaceFinder) ByName(name string) (*control.Interface, error) {
	for _, networkInterface := range f.interfaces {
		if networkInterface.Name == name {
			return &networkInterface, nil
		}
	}
	return nil, errors.New("not found")
}

func (f *mockInterfaceFinder) ByIndex(index int) (*control.Interface, error) {
	for _, networkInterface := range f.interfaces {
		if networkInterface.Index == index {
			return &networkInterface, nil
		}
	}
	return nil, errors.New("not found")
}

func (f *mockInterfaceFinder) ByAddr(addr netip.Addr) (*control.Interface, error) {
	for _, networkInterface := range f.interfaces {
		for _, prefix := range networkInterface.Addresses {
			if prefix.Contains(addr) {
				return &networkInterface, nil
			}
		}
	}
	return nil, errors.New("not found")
}

type mockNetworkUpdateMonitor struct {
	access    list.List[tun.NetworkUpdateCallback]
	accessMux sync.Mutex
}

func (m *mockNetworkUpdateMonitor) Start() error { return nil }

func (m *mockNetworkUpdateMonitor) Close() error { return nil }

func (m *mockNetworkUpdateMonitor) RegisterCallback(callback tun.NetworkUpdateCallback) *list.Element[tun.NetworkUpdateCallback] {
	m.accessMux.Lock()
	defer m.accessMux.Unlock()
	return m.access.PushBack(callback)
}

func (m *mockNetworkUpdateMonitor) UnregisterCallback(element *list.Element[tun.NetworkUpdateCallback]) {
	m.accessMux.Lock()
	defer m.accessMux.Unlock()
	m.access.Remove(element)
}

func (m *mockNetworkUpdateMonitor) Emit() {
	m.accessMux.Lock()
	callbacks := m.access.Array()
	m.accessMux.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}
