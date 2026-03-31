package dialer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/control"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/database64128/tfo-go/v2"
)

var (
	_ ParallelInterfaceDialer = (*DefaultDialer)(nil)
	_ WireGuardListener       = (*DefaultDialer)(nil)
)

type DefaultDialer struct {
	dialer4                 tfo.Dialer
	dialer6                 tfo.Dialer
	udpDialer4              net.Dialer
	udpDialer6              net.Dialer
	udpListener4            net.ListenConfig
	udpListener6            net.ListenConfig
	udpAddr4                string
	udpAddr6                string
	netns                   string
	connectionManager       adapter.ConnectionManager
	networkManager          adapter.NetworkManager
	networkStrategy4        *C.NetworkStrategy
	networkStrategy6        *C.NetworkStrategy
	defaultNetworkStrategy4 bool
	defaultNetworkStrategy6 bool
	networkType             []C.InterfaceType
	fallbackNetworkType     []C.InterfaceType
	networkFallbackDelay    time.Duration
	networkLastFallback     common.TypedValue[time.Time]
}

func NewDefault(ctx context.Context, options option.DialerOptions) (*DefaultDialer, error) {
	connectionManager := service.FromContext[adapter.ConnectionManager](ctx)
	networkManager := service.FromContext[adapter.NetworkManager](ctx)
	platformInterface := service.FromContext[adapter.PlatformInterface](ctx)

	var (
		dialer               net.Dialer
		listener             net.ListenConfig
		interfaceFinder      control.InterfaceFinder
		networkStrategy4     *C.NetworkStrategy
		networkStrategy6     *C.NetworkStrategy
		defaultStrategy4     bool
		defaultStrategy6     bool
		networkType          []C.InterfaceType
		fallbackNetworkType  []C.InterfaceType
		networkFallbackDelay time.Duration
		bindPlan             defaultBindPlan
	)
	if networkManager != nil {
		interfaceFinder = networkManager.InterfaceFinder()
	} else {
		interfaceFinder = control.NewDefaultInterfaceFinder()
	}
	if options.BindInterface != "" {
		if !(C.IsLinux || C.IsDarwin || C.IsWindows) {
			return nil, E.New("`bind_interface` is only supported on Linux, macOS and Windows")
		}
		bindFunc := control.BindToInterface(interfaceFinder, options.BindInterface, -1)
		dialer.Control = control.Append(dialer.Control, bindFunc)
		listener.Control = control.Append(listener.Control, bindFunc)
	}
	if options.RoutingMark > 0 {
		if !C.IsLinux {
			return nil, E.New("`routing_mark` is only supported on Linux")
		}
		dialer.Control = control.Append(dialer.Control, setMarkWrapper(networkManager, uint32(options.RoutingMark), false))
		listener.Control = control.Append(listener.Control, setMarkWrapper(networkManager, uint32(options.RoutingMark), false))
	}
	disableDefaultBind := options.BindInterface != "" || options.Inet4BindAddress != nil || options.Inet6BindAddress != nil
	if disableDefaultBind || options.TCPFastOpen {
		if options.NetworkStrategy != nil || len(options.NetworkType) > 0 && options.FallbackNetworkType == nil && options.FallbackDelay == 0 {
			return nil, E.New("`network_strategy` is conflict with `bind_interface`, `inet4_bind_address`, `inet6_bind_address` and `tcp_fast_open`")
		}
	}

	if networkManager != nil {
		defaultOptions := networkManager.DefaultOptions()
		bindPlan = resolveDefaultBindPlan(disableDefaultBind, networkManager.AutoDetectInterface(), platformInterface != nil, defaultOptions)
		if bindPlan.mode4 == bindModeProtect || bindPlan.mode6 == bindModeProtect {
			resolvedStrategy := (*C.NetworkStrategy)(options.NetworkStrategy)
			networkType = common.Map(options.NetworkType, option.InterfaceType.Build)
			fallbackNetworkType = common.Map(options.FallbackNetworkType, option.InterfaceType.Build)
			if resolvedStrategy == nil && len(networkType) == 0 && len(fallbackNetworkType) == 0 {
				resolvedStrategy = defaultOptions.NetworkStrategy
				networkType = defaultOptions.NetworkType
				fallbackNetworkType = defaultOptions.FallbackNetworkType
			}
			networkFallbackDelay = time.Duration(options.FallbackDelay)
			if networkFallbackDelay == 0 && defaultOptions.FallbackDelay != 0 {
				networkFallbackDelay = defaultOptions.FallbackDelay
			}
			if bindPlan.mode4 == bindModeProtect {
				if resolvedStrategy == nil {
					networkStrategy4 = common.Ptr(C.NetworkStrategyDefault)
					defaultStrategy4 = true
				} else {
					networkStrategy4 = resolvedStrategy
				}
			}
			if bindPlan.mode6 == bindModeProtect {
				if resolvedStrategy == nil {
					networkStrategy6 = common.Ptr(C.NetworkStrategyDefault)
					defaultStrategy6 = true
				} else {
					networkStrategy6 = resolvedStrategy
				}
			}
		}
		if options.RoutingMark == 0 && defaultOptions.RoutingMark != 0 {
			dialer.Control = control.Append(dialer.Control, setMarkWrapper(networkManager, defaultOptions.RoutingMark, true))
			listener.Control = control.Append(listener.Control, setMarkWrapper(networkManager, defaultOptions.RoutingMark, true))
		}
	}
	if networkManager != nil {
		markFunc := networkManager.AutoRedirectOutputMarkFunc()
		dialer.Control = control.Append(dialer.Control, markFunc)
		listener.Control = control.Append(listener.Control, markFunc)
	}
	if options.ReuseAddr {
		listener.Control = control.Append(listener.Control, control.ReuseAddr())
	}
	if options.ProtectPath != "" {
		dialer.Control = control.Append(dialer.Control, control.ProtectPath(options.ProtectPath))
		listener.Control = control.Append(listener.Control, control.ProtectPath(options.ProtectPath))
	}
	if options.BindAddressNoPort {
		if !C.IsLinux {
			return nil, E.New("`bind_address_no_port` is only supported on Linux")
		}
		dialer.Control = control.Append(dialer.Control, control.BindAddressNoPort())
	}
	if options.ConnectTimeout != 0 {
		dialer.Timeout = time.Duration(options.ConnectTimeout)
	} else {
		dialer.Timeout = C.TCPConnectTimeout
	}
	if !options.DisableTCPKeepAlive {
		keepIdle := time.Duration(options.TCPKeepAlive)
		if keepIdle == 0 {
			keepIdle = C.TCPKeepAliveInitial
		}
		keepInterval := time.Duration(options.TCPKeepAliveInterval)
		if keepInterval == 0 {
			keepInterval = C.TCPKeepAliveInterval
		}
		dialer.KeepAliveConfig = net.KeepAliveConfig{
			Enable:   true,
			Idle:     keepIdle,
			Interval: keepInterval,
		}
	}
	var udpFragment bool
	if options.UDPFragment != nil {
		udpFragment = *options.UDPFragment
	} else {
		udpFragment = options.UDPFragmentDefault
	}
	if !udpFragment {
		dialer.Control = control.Append(dialer.Control, control.DisableUDPFragment())
		listener.Control = control.Append(listener.Control, control.DisableUDPFragment())
	}
	var (
		dialer4      = dialer
		udpDialer4   = dialer
		udpListener4 = listener
		udpAddr4     string
	)
	appendBindControl := func(targetDialer *net.Dialer, targetUDPDialer *net.Dialer, targetListener *net.ListenConfig, mode bindMode, interfaceName string) {
		if networkManager == nil {
			return
		}
		bindFunc := bindControlForMode(mode, networkManager.InterfaceFinder(), interfaceName, networkManager.AutoDetectInterfaceFunc(), networkManager.ProtectFunc())
		targetDialer.Control = control.Append(targetDialer.Control, bindFunc)
		targetUDPDialer.Control = control.Append(targetUDPDialer.Control, bindFunc)
		targetListener.Control = control.Append(targetListener.Control, bindFunc)
	}
	appendBindControl(&dialer4, &udpDialer4, &udpListener4, bindPlan.mode4, bindPlan.interface4)
	if options.Inet4BindAddress != nil {
		bindAddr := options.Inet4BindAddress.Build(netip.IPv4Unspecified())
		dialer4.LocalAddr = &net.TCPAddr{IP: bindAddr.AsSlice()}
		udpDialer4.LocalAddr = &net.UDPAddr{IP: bindAddr.AsSlice()}
		udpAddr4 = M.SocksaddrFrom(bindAddr, 0).String()
	}
	var (
		dialer6      = dialer
		udpDialer6   = dialer
		udpListener6 = listener
		udpAddr6     string
	)
	appendBindControl(&dialer6, &udpDialer6, &udpListener6, bindPlan.mode6, bindPlan.interface6)
	if options.Inet6BindAddress != nil {
		bindAddr := options.Inet6BindAddress.Build(netip.IPv6Unspecified())
		dialer6.LocalAddr = &net.TCPAddr{IP: bindAddr.AsSlice()}
		udpDialer6.LocalAddr = &net.UDPAddr{IP: bindAddr.AsSlice()}
		udpAddr6 = M.SocksaddrFrom(bindAddr, 0).String()
	}
	if options.TCPMultiPath {
		dialer4.SetMultipathTCP(true)
	}
	tcpDialer4 := tfo.Dialer{Dialer: dialer4, DisableTFO: !options.TCPFastOpen}
	tcpDialer6 := tfo.Dialer{Dialer: dialer6, DisableTFO: !options.TCPFastOpen}
	return &DefaultDialer{
		dialer4:                 tcpDialer4,
		dialer6:                 tcpDialer6,
		udpDialer4:              udpDialer4,
		udpDialer6:              udpDialer6,
		udpListener4:            udpListener4,
		udpListener6:            udpListener6,
		udpAddr4:                udpAddr4,
		udpAddr6:                udpAddr6,
		netns:                   options.NetNs,
		connectionManager:       connectionManager,
		networkManager:          networkManager,
		networkStrategy4:        networkStrategy4,
		networkStrategy6:        networkStrategy6,
		defaultNetworkStrategy4: defaultStrategy4,
		defaultNetworkStrategy6: defaultStrategy6,
		networkType:             networkType,
		fallbackNetworkType:     fallbackNetworkType,
		networkFallbackDelay:    networkFallbackDelay,
	}, nil
}

func setMarkWrapper(networkManager adapter.NetworkManager, mark uint32, isDefault bool) control.Func {
	if networkManager == nil {
		return control.RoutingMark(mark)
	}
	return func(network, address string, conn syscall.RawConn) error {
		if networkManager.AutoRedirectOutputMark() != 0 {
			if isDefault {
				return E.New("`route.default_mark` is conflict with `tun.auto_redirect`")
			} else {
				return E.New("`routing_mark` is conflict with `tun.auto_redirect`")
			}
		}
		return control.RoutingMark(mark)(network, address, conn)
	}
}

func (d *DefaultDialer) DialContext(ctx context.Context, network string, address M.Socksaddr) (net.Conn, error) {
	if !address.IsValid() {
		return nil, E.New("invalid address")
	} else if address.IsDomain() {
		return nil, E.New("domain not resolved")
	}
	strategy, _ := d.strategyForDestination(address)
	if strategy == nil {
		return d.trackConn(listener.ListenNetworkNamespace[net.Conn](d.netns, func() (net.Conn, error) {
			switch N.NetworkName(network) {
			case N.NetworkUDP:
				dialer := d.dialerForDestination(network, address)
				return dialer.DialContext(ctx, network, address.String())
			}
			if address.IsIPv6() {
				return DialSlowContext(&d.dialer6, ctx, network, address)
			}
			return DialSlowContext(&d.dialer4, ctx, network, address)
		}))
	}
	return d.DialParallelInterface(ctx, network, address, nil, d.networkType, d.fallbackNetworkType, d.networkFallbackDelay)
}

func (d *DefaultDialer) DialParallelInterface(ctx context.Context, network string, address M.Socksaddr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if strategy == nil {
		strategy, _ = d.strategyForDestination(address)
	}
	if strategy == nil {
		return d.trackConn(listener.ListenNetworkNamespace[net.Conn](d.netns, func() (net.Conn, error) {
			switch N.NetworkName(network) {
			case N.NetworkUDP:
				dialer := d.dialerForDestination(network, address)
				return dialer.DialContext(ctx, network, address.String())
			}
			if address.IsIPv6() {
				return DialSlowContext(&d.dialer6, ctx, network, address)
			}
			return DialSlowContext(&d.dialer4, ctx, network, address)
		}))
	}
	if len(interfaceType) == 0 {
		interfaceType = d.networkType
	}
	if len(fallbackInterfaceType) == 0 {
		fallbackInterfaceType = d.fallbackNetworkType
	}
	if fallbackDelay == 0 {
		fallbackDelay = d.networkFallbackDelay
	}
	dialer := d.dialerForDestination(network, address)
	fastFallback := time.Since(d.networkLastFallback.Load()) < C.TCPTimeout
	var (
		conn      net.Conn
		isPrimary bool
		err       error
	)
	if !fastFallback {
		conn, isPrimary, err = d.dialParallelInterface(ctx, dialer, network, address.String(), *strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	} else {
		conn, isPrimary, err = d.dialParallelInterfaceFastFallback(ctx, dialer, network, address.String(), *strategy, interfaceType, fallbackInterfaceType, fallbackDelay, d.networkLastFallback.Store)
	}
	if err != nil {
		// bind interface failed on legacy xiaomi systems
		_, defaultStrategy := d.strategyForDestination(address)
		if defaultStrategy && errors.Is(err, syscall.EPERM) {
			d.clearDefaultStrategy(address)
			return d.DialContext(ctx, network, address)
		} else {
			return nil, err
		}
	}
	if !fastFallback && !isPrimary {
		d.networkLastFallback.Store(time.Now())
	}
	return d.trackConn(conn, nil)
}

func (d *DefaultDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	strategy, _ := d.strategyForDestination(destination)
	if strategy == nil {
		return d.trackPacketConn(listener.ListenNetworkNamespace[net.PacketConn](d.netns, func() (net.PacketConn, error) {
			packetListener, network, address := d.packetListenerForDestination(destination)
			return packetListener.ListenPacket(ctx, network, address)
		}))
	}
	return d.ListenSerialInterfacePacket(ctx, destination, nil, d.networkType, d.fallbackNetworkType, d.networkFallbackDelay)
}

func (d *DefaultDialer) DialerForICMPDestination(destination netip.Addr) net.Dialer {
	if !destination.Is6() {
		return d.dialer4.Dialer
	} else {
		return d.dialer6.Dialer
	}
}

func (d *DefaultDialer) ListenSerialInterfacePacket(ctx context.Context, destination M.Socksaddr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.PacketConn, error) {
	if strategy == nil {
		strategy, _ = d.strategyForDestination(destination)
	}
	if strategy == nil {
		return d.ListenPacket(ctx, destination)
	}
	if len(interfaceType) == 0 {
		interfaceType = d.networkType
	}
	if len(fallbackInterfaceType) == 0 {
		fallbackInterfaceType = d.fallbackNetworkType
	}
	if fallbackDelay == 0 {
		fallbackDelay = d.networkFallbackDelay
	}
	packetListener, network, address := d.packetListenerForDestination(destination)
	packetConn, err := d.listenSerialInterfacePacket(ctx, packetListener, network, address, *strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	if err != nil {
		// bind interface failed on legacy xiaomi systems
		_, defaultStrategy := d.strategyForDestination(destination)
		if defaultStrategy && errors.Is(err, syscall.EPERM) {
			d.clearDefaultStrategy(destination)
			return d.ListenPacket(ctx, destination)
		} else {
			return nil, err
		}
	}
	return d.trackPacketConn(packetConn, nil)
}

func (d *DefaultDialer) WireGuardControl() control.Func {
	return func(network, address string, conn syscall.RawConn) error {
		switch network {
		case N.NetworkUDP + "6":
			if d.udpListener6.Control != nil {
				return d.udpListener6.Control(network, address, conn)
			}
		default:
			if d.udpListener4.Control != nil {
				return d.udpListener4.Control(network, address, conn)
			}
		}
		return nil
	}
}

func (d *DefaultDialer) dialerForDestination(network string, destination M.Socksaddr) net.Dialer {
	if N.NetworkName(network) == N.NetworkUDP {
		if destination.IsIPv6() {
			return d.udpDialer6
		}
		return d.udpDialer4
	}
	if destination.IsIPv6() {
		return d.dialer6.Dialer
	}
	return d.dialer4.Dialer
}

func (d *DefaultDialer) packetListenerForDestination(destination M.Socksaddr) (net.ListenConfig, string, string) {
	if destination.IsIPv6() {
		return d.udpListener6, N.NetworkUDP + "6", d.udpAddr6
	}
	if destination.IsIPv4() && !destination.Addr.IsUnspecified() {
		return d.udpListener4, N.NetworkUDP + "4", d.udpAddr4
	}
	return d.udpListener4, N.NetworkUDP, d.udpAddr4
}

func (d *DefaultDialer) strategyForDestination(destination M.Socksaddr) (*C.NetworkStrategy, bool) {
	if destination.IsIPv6() {
		return d.networkStrategy6, d.defaultNetworkStrategy6
	}
	return d.networkStrategy4, d.defaultNetworkStrategy4
}

func (d *DefaultDialer) clearDefaultStrategy(destination M.Socksaddr) {
	if destination.IsIPv6() {
		d.networkStrategy6 = nil
		return
	}
	d.networkStrategy4 = nil
}

func (d *DefaultDialer) trackConn(conn net.Conn, err error) (net.Conn, error) {
	if d.connectionManager == nil || err != nil {
		return conn, err
	}
	return d.connectionManager.TrackConn(conn), nil
}

func (d *DefaultDialer) trackPacketConn(conn net.PacketConn, err error) (net.PacketConn, error) {
	if d.connectionManager == nil || err != nil {
		return conn, err
	}
	return d.connectionManager.TrackPacketConn(conn), nil
}
