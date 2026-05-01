package group

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
)

func RegisterAutoSelector(registry *outbound.Registry) {
	outbound.Register[option.AutoSelectorOutboundOptions](registry, C.TypeAutoSelector, NewAutoSelector)
}

var autoSelectorPlatformWatcherConstructor = newAutoSelectorPlatformWatcher

var (
	_ adapter.OutboundGroup           = (*AutoSelector)(nil)
	_ adapter.SelectableOutboundGroup = (*AutoSelector)(nil)
	_ adapter.ConnectionHandler       = (*AutoSelector)(nil)
	_ adapter.PacketConnectionHandler = (*AutoSelector)(nil)
)

type AutoSelector struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	connection                   adapter.ConnectionManager
	networkManager               adapter.NetworkManager
	logger                       logger.ContextLogger
	matchTag                     string
	fallbackTag                  string
	watchInterface               string
	tags                         []string
	outbounds                    map[string]adapter.Outbound
	selected                     common.TypedValue[adapter.Outbound]
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	networkUpdateCallback        *list.Element[tun.NetworkUpdateCallback]
	platformWatcher              io.Closer
	stateAccess                  sync.Mutex
	lastAutoTag                  string
	manualOverrideTag            string
	recheckAccess                sync.Mutex
	checking                     bool
	dirty                        bool
}

func NewAutoSelector(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.AutoSelectorOutboundOptions) (adapter.Outbound, error) {
	if options.Match == "" {
		return nil, E.New("missing match")
	}
	if options.Fallback == "" {
		return nil, E.New("missing fallback")
	}
	if options.WatchInterface == "" {
		return nil, E.New("missing watch_interface")
	}
	tags := uniqueTags(options.Match, options.Fallback)
	return &AutoSelector{
		Adapter:                      outbound.NewAdapter(C.TypeAutoSelector, tag, nil, tags),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		networkManager:               service.FromContext[adapter.NetworkManager](ctx),
		logger:                       logger,
		matchTag:                     options.Match,
		fallbackTag:                  options.Fallback,
		watchInterface:               options.WatchInterface,
		tags:                         tags,
		outbounds:                    make(map[string]adapter.Outbound, len(tags)),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: options.InterruptExistConnections,
	}, nil
}

func (s *AutoSelector) Network() []string {
	selected := s.selectedOutbound()
	if selected == nil {
		return []string{N.NetworkTCP, N.NetworkUDP}
	}
	return selected.Network()
}

func (s *AutoSelector) Start() error {
	if s.networkManager == nil {
		return E.New("missing network manager")
	}
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		s.outbounds[tag] = detour
	}
	s.recheckNow("start")
	if monitor := s.networkManager.NetworkMonitor(); monitor != nil {
		s.networkUpdateCallback = monitor.RegisterCallback(s.onNetworkUpdate)
	}
	watcher, err := autoSelectorPlatformWatcherConstructor(s)
	if err != nil {
		if monitor := s.networkManager.NetworkMonitor(); monitor != nil && s.networkUpdateCallback != nil {
			monitor.UnregisterCallback(s.networkUpdateCallback)
			s.networkUpdateCallback = nil
		}
		return err
	}
	s.platformWatcher = watcher
	return nil
}

func (s *AutoSelector) Close() error {
	if s.networkManager != nil {
		if monitor := s.networkManager.NetworkMonitor(); monitor != nil && s.networkUpdateCallback != nil {
			monitor.UnregisterCallback(s.networkUpdateCallback)
			s.networkUpdateCallback = nil
		}
	}
	if s.platformWatcher != nil {
		err := s.platformWatcher.Close()
		s.platformWatcher = nil
		return err
	}
	return nil
}

func (s *AutoSelector) Now() string {
	selected := s.selectedOutbound()
	if selected == nil {
		return s.fallbackTag
	}
	return selected.Tag()
}

func (s *AutoSelector) All() []string {
	return s.tags
}

func (s *AutoSelector) SelectOutbound(tag string) bool {
	_, loaded := s.outbounds[tag]
	if !loaded {
		return false
	}
	s.stateAccess.Lock()
	s.manualOverrideTag = tag
	s.stateAccess.Unlock()
	s.switchSelected(tag, true)
	return true
}

func (s *AutoSelector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	selected := s.selectedOutbound()
	if selected == nil {
		return nil, E.New("missing selected outbound")
	}
	conn, err := selected.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *AutoSelector) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	selected := s.selectedOutbound()
	if selected == nil {
		return nil, E.New("missing selected outbound")
	}
	conn, err := selected.ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *AutoSelector) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	selected := s.selectedOutbound()
	if selected == nil {
		common.Close(conn)
		return
	}
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	if outboundHandler, isHandler := selected.(adapter.ConnectionHandler); isHandler {
		outboundHandler.NewConnection(ctx, conn, metadata, onClose)
	} else {
		s.connection.NewConnection(ctx, selected, conn, metadata, onClose)
	}
}

func (s *AutoSelector) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	selected := s.selectedOutbound()
	if selected == nil {
		common.Close(conn)
		return
	}
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	if outboundHandler, isHandler := selected.(adapter.PacketConnectionHandler); isHandler {
		outboundHandler.NewPacketConnection(ctx, conn, metadata, onClose)
	} else {
		s.connection.NewPacketConnection(ctx, selected, conn, metadata, onClose)
	}
}

func (s *AutoSelector) NewDirectRouteConnection(metadata adapter.InboundContext, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	selected := s.selectedOutbound()
	if selected == nil {
		return nil, E.New("missing selected outbound")
	}
	if !common.Contains(selected.Network(), metadata.Network) {
		return nil, E.New(metadata.Network, " is not supported by outbound: ", selected.Tag())
	}
	return selected.(adapter.DirectRouteOutbound).NewDirectRouteConnection(metadata, routeContext, timeout)
}

func (s *AutoSelector) onNetworkUpdate() {
	s.logger.Debug("auto selector ", s.Tag(), " received network update")
	s.scheduleRecheck("network-update")
}

func (s *AutoSelector) scheduleRecheck(reason string) {
	s.recheckAccess.Lock()
	if s.checking {
		s.dirty = true
		s.recheckAccess.Unlock()
		return
	}
	s.checking = true
	s.recheckAccess.Unlock()
	go s.recheckLoop(reason)
}

func (s *AutoSelector) recheckLoop(reason string) {
	for {
		s.recheckNow(reason)
		s.recheckAccess.Lock()
		if !s.dirty {
			s.checking = false
			s.recheckAccess.Unlock()
			return
		}
		s.dirty = false
		s.recheckAccess.Unlock()
		reason = "pending"
	}
}

func (s *AutoSelector) recheckNow(reason string) {
	autoTag := s.evaluateAutoTag(reason)
	s.applyAutoSelection(autoTag, reason)
}

func (s *AutoSelector) evaluateAutoTag(reason string) string {
	s.logger.Debug("auto selector ", s.Tag(), " re-evaluating ", s.watchInterface, " due to ", reason)
	if err := s.networkManager.UpdateInterfaces(); err != nil {
		s.logger.Debug("auto selector ", s.Tag(), " update interfaces: ", err)
	}
	if _, err := s.networkManager.InterfaceFinder().ByName(s.watchInterface); err == nil {
		return s.matchTag
	}
	return s.fallbackTag
}

func (s *AutoSelector) applyAutoSelection(autoTag string, reason string) {
	s.stateAccess.Lock()
	autoChanged := autoTag != s.lastAutoTag
	if autoChanged {
		s.lastAutoTag = autoTag
		s.manualOverrideTag = ""
	}
	selectedTag := autoTag
	if !autoChanged && s.manualOverrideTag != "" {
		selectedTag = s.manualOverrideTag
	}
	s.stateAccess.Unlock()
	if autoChanged {
		if autoTag == s.matchTag {
			s.logger.Info("auto selector ", s.Tag(), " matched watch interface ", s.watchInterface, " due to ", reason)
		} else {
			s.logger.Info("auto selector ", s.Tag(), " fell back because watch interface ", s.watchInterface, " is missing")
		}
	}
	s.switchSelected(selectedTag, false)
}

func (s *AutoSelector) switchSelected(tag string, isManual bool) {
	detour, loaded := s.outbounds[tag]
	if !loaded {
		return
	}
	old := s.selected.Swap(detour)
	if old == detour {
		return
	}
	if isManual {
		s.logger.Info("auto selector ", s.Tag(), " manual override selected ", tag)
	}
	if old != nil {
		s.interruptGroup.Interrupt(s.interruptExternalConnections)
	}
}

func (s *AutoSelector) selectedOutbound() adapter.Outbound {
	if selected := s.selected.Load(); selected != nil {
		return selected
	}
	return s.outbounds[s.fallbackTag]
}

func uniqueTags(tags ...string) []string {
	var unique []string
	for _, tag := range tags {
		if tag == "" || common.Contains(unique, tag) {
			continue
		}
		unique = append(unique, tag)
	}
	return unique
}
