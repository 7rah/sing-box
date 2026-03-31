package dialer

import (
	"net"
	"net/netip"
	"syscall"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"

	"github.com/database64128/tfo-go/v2"
)

func TestDialerForDestinationUsesIPv6Dialer(t *testing.T) {
	var calls []string
	d := &DefaultDialer{
		dialer4:    tfo.Dialer{Dialer: net.Dialer{Control: recordControl("tcp4", &calls)}},
		dialer6:    tfo.Dialer{Dialer: net.Dialer{Control: recordControl("tcp6", &calls)}},
		udpDialer4: net.Dialer{Control: recordControl("udp4", &calls)},
		udpDialer6: net.Dialer{Control: recordControl("udp6", &calls)},
	}

	remoteTCP := M.SocksaddrFrom(netip.MustParseAddr("2001:db8::1"), 443)
	tcpDialer := d.dialerForDestination("tcp", remoteTCP)
	require.NoError(t, tcpDialer.Control("tcp6", remoteTCP.String(), noopRawConn{}))

	remoteUDP := M.SocksaddrFrom(netip.MustParseAddr("2001:db8::2"), 53)
	udpDialer := d.dialerForDestination("udp", remoteUDP)
	require.NoError(t, udpDialer.Control("udp6", remoteUDP.String(), noopRawConn{}))

	require.Equal(t, []string{"tcp6", "udp6"}, calls)
}

func TestPacketListenerForDestinationUsesIPv6Listener(t *testing.T) {
	var calls []string
	d := &DefaultDialer{
		udpListener4: net.ListenConfig{Control: recordControl("v4", &calls)},
		udpListener6: net.ListenConfig{Control: recordControl("v6", &calls)},
		udpAddr4:     "0.0.0.0:0",
		udpAddr6:     "[::]:0",
	}

	listener, network, address := d.packetListenerForDestination(M.SocksaddrFrom(netip.MustParseAddr("2001:db8::1"), 53))
	require.Equal(t, "udp6", network)
	require.Equal(t, "[::]:0", address)
	require.NoError(t, listener.Control(network, address, noopRawConn{}))
	require.Equal(t, []string{"v6"}, calls)
}

func TestPacketListenerForDestinationKeepsIPv4ListenerForUnspecifiedDestination(t *testing.T) {
	var calls []string
	d := &DefaultDialer{
		udpListener4: net.ListenConfig{Control: recordControl("v4", &calls)},
		udpListener6: net.ListenConfig{Control: recordControl("v6", &calls)},
		udpAddr4:     "0.0.0.0:0",
		udpAddr6:     "[::]:0",
	}

	listener, network, address := d.packetListenerForDestination(M.Socksaddr{})
	require.Equal(t, "udp", network)
	require.Equal(t, "0.0.0.0:0", address)
	require.NoError(t, listener.Control(network, address, noopRawConn{}))
	require.Equal(t, []string{"v4"}, calls)
}

func TestWireGuardControlDispatchesByFamily(t *testing.T) {
	var calls []string
	d := &DefaultDialer{
		udpListener4: net.ListenConfig{Control: recordControl("v4", &calls)},
		udpListener6: net.ListenConfig{Control: recordControl("v6", &calls)},
	}

	controlFn := d.WireGuardControl()
	require.NoError(t, controlFn("udp4", ":0", noopRawConn{}))
	require.NoError(t, controlFn("udp6", "[::]:0", noopRawConn{}))
	require.Equal(t, []string{"v4", "v6"}, calls)
}

func recordControl(label string, calls *[]string) func(network, address string, conn syscall.RawConn) error {
	return func(network, address string, conn syscall.RawConn) error {
		*calls = append(*calls, label)
		return nil
	}
}

type noopRawConn struct{}

func (noopRawConn) Control(fn func(fd uintptr)) error {
	fn(0)
	return nil
}

func (noopRawConn) Read(func(fd uintptr) bool) error {
	return nil
}

func (noopRawConn) Write(func(fd uintptr) bool) error {
	return nil
}
