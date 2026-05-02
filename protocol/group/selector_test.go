package group

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestSelectorLoadBalanceRoundRobinSelectedOutboundInstances(t *testing.T) {
	manager := newTestSelectorOutboundManager("a", "b")
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
	selector, err := NewSelector(ctx, nil, nil, "select", option.SelectorOutboundOptions{
		Outbounds: []string{"a", "b"},
		Default:   "a",
		LoadBalance: option.SelectorLoadBalanceOptions{
			Enabled:   true,
			Instances: 3,
			Strategy:  "round_robin",
		},
	})
	require.NoError(t, err)
	lifecycle := selector.(adapter.Lifecycle)
	require.NoError(t, lifecycle.Start(adapter.StartStateInitialize))
	require.Empty(t, manager.createdTags())
	require.NoError(t, lifecycle.Start(adapter.StartStateStart))
	require.Equal(t, []string{"select/a/1", "select/a/2"}, manager.createdTags())
	require.Equal(t, []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStateStart}, manager.startedStages("select/a/1"))
	require.Equal(t, []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStateStart}, manager.startedStages("select/a/2"))

	for range 4 {
		conn, err := selector.DialContext(context.Background(), N.NetworkTCP, M.Socksaddr{})
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	require.Equal(t, []string{"a", "select/a/1", "select/a/2", "a"}, manager.dialedTags())
	require.NoError(t, lifecycle.Start(adapter.StartStatePostStart))
	require.NoError(t, lifecycle.Start(adapter.StartStateStarted))
	allStages := []adapter.StartStage{
		adapter.StartStateInitialize,
		adapter.StartStateStart,
		adapter.StartStatePostStart,
		adapter.StartStateStarted,
	}
	require.Equal(t, allStages, manager.startedStages("select/a/1"))
	require.Equal(t, allStages, manager.startedStages("select/a/2"))

	require.True(t, selector.(*Selector).SelectOutbound("b"))
	require.Empty(t, manager.closedTags())
	require.Equal(t, []string{"select/a/1", "select/a/2", "select/b/1", "select/b/2"}, manager.createdTags())
	require.Equal(t, allStages, manager.startedStages("select/b/1"))
	require.Equal(t, allStages, manager.startedStages("select/b/2"))
	for range 3 {
		conn, err := selector.DialContext(context.Background(), N.NetworkTCP, M.Socksaddr{})
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	require.Equal(t, []string{"a", "select/a/1", "select/a/2", "a", "b", "select/b/1", "select/b/2"}, manager.dialedTags())
}

func TestSelectorLoadBalanceClosesPreviousInstancesWhenInterruptingConnections(t *testing.T) {
	manager := newTestSelectorOutboundManager("a", "b")
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
	selector, err := NewSelector(ctx, nil, nil, "select", option.SelectorOutboundOptions{
		Outbounds: []string{"a", "b"},
		Default:   "a",
		LoadBalance: option.SelectorLoadBalanceOptions{
			Enabled:   true,
			Instances: 3,
			Strategy:  "round_robin",
		},
		InterruptExistConnections: true,
	})
	require.NoError(t, err)
	lifecycle := selector.(adapter.Lifecycle)
	require.NoError(t, lifecycle.Start(adapter.StartStateInitialize))
	require.NoError(t, lifecycle.Start(adapter.StartStateStart))
	require.Equal(t, []string{"select/a/1", "select/a/2"}, manager.createdTags())

	require.True(t, selector.(*Selector).SelectOutbound("b"))
	require.Equal(t, []string{"select/a/1", "select/a/2"}, manager.closedTags())
	require.Equal(t, []string{"select/a/1", "select/a/2", "select/b/1", "select/b/2"}, manager.createdTags())

	require.True(t, selector.(*Selector).SelectOutbound("a"))
	require.Equal(t, []string{"select/a/1", "select/a/2", "select/b/1", "select/b/2"}, manager.closedTags())
	require.Equal(t, []string{"select/a/1", "select/a/2", "select/b/1", "select/b/2", "select/a/1", "select/a/2"}, manager.createdTags())
}

type testSelectorOutboundManager struct {
	outbounds map[string]*testSelectorOutbound
	access    sync.Mutex
	created   []string
	started   map[string][]adapter.StartStage
	dialed    []string
	closed    []string
}

func newTestSelectorOutboundManager(tags ...string) *testSelectorOutboundManager {
	manager := &testSelectorOutboundManager{
		outbounds: make(map[string]*testSelectorOutbound),
		started:   make(map[string][]adapter.StartStage),
	}
	for _, tag := range tags {
		manager.outbounds[tag] = &testSelectorOutbound{tag: tag, manager: manager}
	}
	return manager
}

func (m *testSelectorOutboundManager) Start(adapter.StartStage) error { return nil }

func (m *testSelectorOutboundManager) Close() error { return nil }

func (m *testSelectorOutboundManager) Outbounds() []adapter.Outbound {
	return nil
}

func (m *testSelectorOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, loaded := m.outbounds[tag]
	return outbound, loaded
}

func (m *testSelectorOutboundManager) Default() adapter.Outbound { return nil }

func (m *testSelectorOutboundManager) Remove(string) error { return nil }

func (m *testSelectorOutboundManager) Create(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}

func (m *testSelectorOutboundManager) CreateInstance(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, sourceTag string) (adapter.Outbound, error) {
	_, loaded := m.outbounds[sourceTag]
	if !loaded {
		return nil, io.ErrUnexpectedEOF
	}
	m.access.Lock()
	m.created = append(m.created, tag)
	m.access.Unlock()
	return &testSelectorOutbound{tag: tag, manager: m}, nil
}

func (m *testSelectorOutboundManager) recordStart(tag string, stage adapter.StartStage) {
	m.access.Lock()
	defer m.access.Unlock()
	m.started[tag] = append(m.started[tag], stage)
}

func (m *testSelectorOutboundManager) recordDial(tag string) {
	m.access.Lock()
	defer m.access.Unlock()
	m.dialed = append(m.dialed, tag)
}

func (m *testSelectorOutboundManager) recordClose(tag string) {
	m.access.Lock()
	defer m.access.Unlock()
	m.closed = append(m.closed, tag)
}

func (m *testSelectorOutboundManager) dialedTags() []string {
	m.access.Lock()
	defer m.access.Unlock()
	return append([]string(nil), m.dialed...)
}

func (m *testSelectorOutboundManager) createdTags() []string {
	m.access.Lock()
	defer m.access.Unlock()
	return append([]string(nil), m.created...)
}

func (m *testSelectorOutboundManager) closedTags() []string {
	m.access.Lock()
	defer m.access.Unlock()
	return append([]string(nil), m.closed...)
}

func (m *testSelectorOutboundManager) startedStages(tag string) []adapter.StartStage {
	m.access.Lock()
	defer m.access.Unlock()
	return append([]adapter.StartStage(nil), m.started[tag]...)
}

type testSelectorOutbound struct {
	tag     string
	manager *testSelectorOutboundManager
}

func (o *testSelectorOutbound) Type() string { return "test" }

func (o *testSelectorOutbound) Tag() string { return o.tag }

func (o *testSelectorOutbound) Network() []string { return []string{N.NetworkTCP, N.NetworkUDP} }

func (o *testSelectorOutbound) Dependencies() []string { return nil }

func (o *testSelectorOutbound) Start(stage adapter.StartStage) error {
	o.manager.recordStart(o.tag, stage)
	return nil
}

func (o *testSelectorOutbound) Close() error {
	o.manager.recordClose(o.tag)
	return nil
}

func (o *testSelectorOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.manager.recordDial(o.tag)
	return testSelectorConn{}, nil
}

func (o *testSelectorOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, io.ErrUnexpectedEOF
}

type testSelectorConn struct{}

func (testSelectorConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (testSelectorConn) Write(p []byte) (int, error)      { return len(p), nil }
func (testSelectorConn) Close() error                     { return nil }
func (testSelectorConn) LocalAddr() net.Addr              { return nil }
func (testSelectorConn) RemoteAddr() net.Addr             { return nil }
func (testSelectorConn) SetDeadline(time.Time) error      { return nil }
func (testSelectorConn) SetReadDeadline(time.Time) error  { return nil }
func (testSelectorConn) SetWriteDeadline(time.Time) error { return nil }
