package group

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
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
	"github.com/sagernet/sing/service"
)

func RegisterSelector(registry *outbound.Registry) {
	outbound.Register[option.SelectorOutboundOptions](registry, C.TypeSelector, NewSelector)
}

var (
	_ adapter.OutboundGroup             = (*Selector)(nil)
	_ adapter.ConnectionHandlerEx       = (*Selector)(nil)
	_ adapter.PacketConnectionHandlerEx = (*Selector)(nil)
)

type Selector struct {
	outbound.Adapter
	ctx                          context.Context
	router                       adapter.Router
	outbound                     adapter.OutboundManager
	connection                   adapter.ConnectionManager
	logger                       logger.ContextLogger
	tags                         []string
	defaultTag                   string
	loadBalance                  option.SelectorLoadBalanceOptions
	outbounds                    map[string]adapter.Outbound
	selected                     common.TypedValue[*selectorSelection]
	loadBalanceAccess            sync.Mutex
	loadBalanceGroups            map[string]*selectorLoadBalanceGroup
	lifecycleStage               atomic.Uint32
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
}

type selectorSelection struct {
	outbound adapter.Outbound
	group    *selectorLoadBalanceGroup
}

type selectorLoadBalanceGroup struct {
	outbounds []adapter.Outbound
	index     atomic.Uint64
}

func NewSelector(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SelectorOutboundOptions) (adapter.Outbound, error) {
	outbound := &Selector{
		Adapter:                      outbound.NewAdapter(C.TypeSelector, tag, nil, options.Outbounds),
		ctx:                          ctx,
		router:                       router,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		defaultTag:                   options.Default,
		loadBalance:                  options.LoadBalance,
		outbounds:                    make(map[string]adapter.Outbound),
		loadBalanceGroups:            make(map[string]*selectorLoadBalanceGroup),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}
	if outbound.loadBalance.Enabled {
		if outbound.loadBalance.Instances < 1 {
			return nil, E.New("load_balance.instances must be greater than zero")
		}
		if outbound.loadBalance.Strategy == "" {
			outbound.loadBalance.Strategy = "round_robin"
		}
		if outbound.loadBalance.Strategy != "round_robin" {
			return nil, E.New("unknown load_balance.strategy: ", outbound.loadBalance.Strategy)
		}
	}
	return outbound, nil
}

func (s *Selector) Network() []string {
	selection := s.selected.Load()
	if selection == nil {
		return []string{N.NetworkTCP, N.NetworkUDP}
	}
	return selection.outbound.Network()
}

func (s *Selector) Start(stage adapter.StartStage) error {
	s.lifecycleStage.Store(uint32(stage))
	switch stage {
	case adapter.StartStateStart:
		return s.start()
	case adapter.StartStatePostStart, adapter.StartStateStarted:
		return s.startLoadBalanceInstances(stage)
	default:
		return nil
	}
}

func (s *Selector) start() error {
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		s.outbounds[tag] = detour
	}

	if s.Tag() != "" {
		cacheFile := service.FromContext[adapter.CacheFile](s.ctx)
		if cacheFile != nil {
			selected := cacheFile.LoadSelected(s.Tag())
			if selected != "" {
				detour, loaded := s.outbounds[selected]
				if loaded {
					return s.storeSelected(detour)
				}
			}
		}
	}

	if s.defaultTag != "" {
		detour, loaded := s.outbounds[s.defaultTag]
		if !loaded {
			return E.New("default outbound not found: ", s.defaultTag)
		}
		return s.storeSelected(detour)
	}

	return s.storeSelected(s.outbounds[s.tags[0]])
}

func (s *Selector) createLoadBalanceGroup(tag string) (*selectorLoadBalanceGroup, error) {
	s.loadBalanceAccess.Lock()
	defer s.loadBalanceAccess.Unlock()

	group := s.loadBalanceGroups[tag]
	if group != nil {
		return group, nil
	}
	detour := s.outbounds[tag]
	group = &selectorLoadBalanceGroup{
		outbounds: []adapter.Outbound{detour},
	}
	for i := 1; i < s.loadBalance.Instances; i++ {
		instanceTag := s.loadBalanceInstanceTag(tag, i)
		instanceCtx := adapter.WithContext(s.ctx, &adapter.InboundContext{
			Outbound: instanceTag,
		})
		instance, err := s.outbound.CreateInstance(instanceCtx, s.router, s.logger, instanceTag, tag)
		if err != nil {
			common.Close(group)
			return nil, E.Cause(err, "create load balance instance[", instanceTag, "]")
		}
		err = s.startLoadBalanceInstance(instance)
		if err != nil {
			common.Close(instance)
			common.Close(group)
			return nil, E.Cause(err, "start load balance instance[", instanceTag, "]")
		}
		group.outbounds = append(group.outbounds, instance)
	}
	s.loadBalanceGroups[tag] = group
	return group, nil
}

func (g *selectorLoadBalanceGroup) Close() error {
	var err error
	for _, instance := range g.outbounds[1:] {
		err = E.Append(err, common.Close(instance), func(err error) error {
			return E.Cause(err, "close load balance instance[", instance.Tag(), "]")
		})
	}
	return err
}

func (s *Selector) startLoadBalanceInstance(instance adapter.Outbound) error {
	currentStage := adapter.StartStage(s.lifecycleStage.Load())
	for _, stage := range adapter.ListStartStages {
		if stage > currentStage {
			break
		}
		err := adapter.LegacyStart(instance, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Selector) startLoadBalanceInstances(stage adapter.StartStage) error {
	if !s.loadBalance.Enabled {
		return nil
	}
	s.loadBalanceAccess.Lock()
	groups := make([]*selectorLoadBalanceGroup, 0, len(s.loadBalanceGroups))
	for _, group := range s.loadBalanceGroups {
		groups = append(groups, group)
	}
	s.loadBalanceAccess.Unlock()

	for _, group := range groups {
		for _, instance := range group.outbounds[1:] {
			err := adapter.LegacyStart(instance, stage)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Selector) loadBalanceInstanceTag(tag string, index int) string {
	if s.Tag() == "" {
		return tag + "/" + strconv.Itoa(index)
	}
	return s.Tag() + "/" + tag + "/" + strconv.Itoa(index)
}

func (s *Selector) storeSelected(detour adapter.Outbound) error {
	var group *selectorLoadBalanceGroup
	if s.loadBalance.Enabled {
		var err error
		group, err = s.createLoadBalanceGroup(detour.Tag())
		if err != nil {
			return err
		}
	}
	s.selected.Store(&selectorSelection{outbound: detour, group: group})
	return nil
}

func (s *Selector) Now() string {
	selection := s.selected.Load()
	if selection == nil {
		return s.tags[0]
	}
	return selection.outbound.Tag()
}

func (s *Selector) All() []string {
	return s.tags
}

func (s *Selector) SelectOutbound(tag string) bool {
	detour, loaded := s.outbounds[tag]
	if !loaded {
		return false
	}
	oldSelection := s.selected.Load()
	if oldSelection != nil && oldSelection.outbound == detour {
		return true
	}
	err := s.storeSelected(detour)
	if err != nil {
		s.logger.Error("create load balance group: ", err)
		return false
	}
	if s.interruptExternalConnections && oldSelection != nil && oldSelection.group != nil {
		s.closeLoadBalanceGroup(oldSelection.outbound.Tag(), oldSelection.group)
	}
	if s.Tag() != "" {
		cacheFile := service.FromContext[adapter.CacheFile](s.ctx)
		if cacheFile != nil {
			err := cacheFile.StoreSelected(s.Tag(), tag)
			if err != nil {
				s.logger.Error("store selected: ", err)
			}
		}
	}
	s.interruptGroup.Interrupt(s.interruptExternalConnections)
	return true
}

func (s *Selector) selectedOutbound() adapter.Outbound {
	selection := s.selected.Load()
	if selection == nil {
		return nil
	}
	if selection.group != nil {
		return selection.group.Select()
	}
	return selection.outbound
}

func (g *selectorLoadBalanceGroup) Select() adapter.Outbound {
	index := g.index.Add(1) - 1
	return g.outbounds[index%uint64(len(g.outbounds))]
}

func (s *Selector) closeLoadBalanceGroup(tag string, group *selectorLoadBalanceGroup) {
	s.loadBalanceAccess.Lock()
	if s.loadBalanceGroups[tag] == group {
		delete(s.loadBalanceGroups, tag)
	}
	s.loadBalanceAccess.Unlock()
	err := group.Close()
	if err != nil {
		s.logger.Error(err)
	}
}

func (s *Selector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	conn, err := s.selectedOutbound().DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *Selector) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	conn, err := s.selectedOutbound().ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *Selector) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	selected := s.selectedOutbound()
	if outboundHandler, isHandler := selected.(adapter.ConnectionHandlerEx); isHandler {
		outboundHandler.NewConnectionEx(ctx, conn, metadata, onClose)
	} else {
		s.connection.NewConnection(ctx, selected, conn, metadata, onClose)
	}
}

func (s *Selector) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	selected := s.selectedOutbound()
	if outboundHandler, isHandler := selected.(adapter.PacketConnectionHandlerEx); isHandler {
		outboundHandler.NewPacketConnectionEx(ctx, conn, metadata, onClose)
	} else {
		s.connection.NewPacketConnection(ctx, selected, conn, metadata, onClose)
	}
}

func (s *Selector) NewDirectRouteConnection(metadata adapter.InboundContext, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	selected := s.selectedOutbound()
	if !common.Contains(selected.Network(), metadata.Network) {
		return nil, E.New(metadata.Network, " is not supported by outbound: ", selected.Tag())
	}
	return selected.(adapter.DirectRouteOutbound).NewDirectRouteConnection(metadata, routeContext, timeout)
}

func (s *Selector) Close() error {
	if !s.loadBalance.Enabled {
		return nil
	}
	s.loadBalanceAccess.Lock()
	groups := make([]*selectorLoadBalanceGroup, 0, len(s.loadBalanceGroups))
	for _, group := range s.loadBalanceGroups {
		groups = append(groups, group)
	}
	s.loadBalanceAccess.Unlock()

	var err error
	for _, group := range groups {
		err = E.Append(err, group.Close(), func(err error) error {
			return err
		})
	}
	return err
}

func RealTag(detour adapter.Outbound) string {
	if group, isGroup := detour.(adapter.OutboundGroup); isGroup {
		return group.Now()
	}
	return detour.Tag()
}
