package v2raygrpclite

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

var defaultClientHeader = http.Header{
	"Content-Type": []string{"application/grpc"},
	"User-Agent":   []string{"grpc-go/1.48.0"},
	"TE":           []string{"trailers"},
}

type Client struct {
	ctx                context.Context
	serverAddr         M.Socksaddr
	transports         []*http2.Transport
	options            option.V2RayGRPCOptions
	url                *url.URL
	host               string
	pinnedDestinations map[string]struct{}
	nextTransport      atomic.Uint32
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayGRPCOptions, tlsConfig tls.Config) adapter.V2RayClientTransport {
	var host string
	if tlsConfig != nil && tlsConfig.ServerName() != "" {
		host = M.ParseSocksaddrHostPort(tlsConfig.ServerName(), serverAddr.Port).String()
	} else {
		host = serverAddr.String()
	}
	var transportCount int
	if options.Pool != nil && options.Pool.Enabled && options.Pool.Size > 0 {
		transportCount = options.Pool.Size
	} else {
		transportCount = 1
	}
	client := &Client{
		ctx:        ctx,
		serverAddr: serverAddr,
		options:    options,
		transports: make([]*http2.Transport, transportCount),
		url: &url.URL{
			Scheme:  "https",
			Host:    serverAddr.String(),
			Path:    "/" + options.ServiceName + "/Tun",
			RawPath: "/" + url.PathEscape(options.ServiceName) + "/Tun",
		},
		host: host,
	}
	if options.Pool != nil && len(options.Pool.PinnedDestinations) > 0 {
		client.pinnedDestinations = make(map[string]struct{}, len(options.Pool.PinnedDestinations))
		for _, destination := range options.Pool.PinnedDestinations {
			destination = strings.TrimSpace(destination)
			if destination == "" {
				continue
			}
			client.pinnedDestinations[destination] = struct{}{}
		}
	}
	var dialTLSContext func(context.Context, string, string, *tls.STDConfig) (net.Conn, error)
	if tlsConfig == nil {
		dialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
			return dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
		}
	} else {
		if len(tlsConfig.NextProtos()) == 0 {
			tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
		}
		tlsDialer := tls.NewDialer(dialer, tlsConfig)
		dialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
			return tlsDialer.DialTLSContext(ctx, M.ParseSocksaddr(addr))
		}
	}
	for i := range client.transports {
		client.transports[i] = &http2.Transport{
			ReadIdleTimeout:    time.Duration(options.IdleTimeout),
			PingTimeout:        time.Duration(options.PingTimeout),
			DisableCompression: true,
			DialTLSContext:     dialTLSContext,
		}
	}

	return client
}

func (c *Client) transportForContext(ctx context.Context) *http2.Transport {
	if metadata := adapter.ContextFrom(ctx); metadata != nil && len(c.pinnedDestinations) > 0 {
		if _, pinned := c.pinnedDestinations[metadata.Destination.String()]; pinned {
			return c.transports[0]
		}
	}
	return c.transports[int(c.nextTransport.Add(1)-1)%len(c.transports)]
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	pipeInReader, pipeInWriter := io.Pipe()
	request := &http.Request{
		Method: http.MethodPost,
		Body:   pipeInReader,
		URL:    c.url,
		Header: defaultClientHeader,
		Host:   c.host,
	}
	request = request.WithContext(ctx)
	conn := newLateGunConn(pipeInWriter)
	conn.setCancel(cancel)
	transport := c.transportForContext(ctx)
	go func() {
		response, err := transport.RoundTrip(request)
		if err != nil {
			_ = pipeInWriter.CloseWithError(err)
			conn.setup(nil, err)
		} else if response.StatusCode != 200 {
			response.Body.Close()
			err = E.New("v2ray-grpc: unexpected status: ", response.Status)
			_ = pipeInWriter.CloseWithError(err)
			conn.setup(nil, err)
		} else {
			conn.setup(response.Body, nil)
		}
	}()
	return conn, nil
}

func (c *Client) Close() error {
	for _, transport := range c.transports {
		v2rayhttp.ResetTransport(transport)
	}
	return nil
}
