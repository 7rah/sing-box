package v2raygrpclite

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)
var _ adapter.V2RayClientTransport = (*pooledClient)(nil)

var defaultClientHeader = http.Header{
	"Content-Type": []string{"application/grpc"},
	"User-Agent":   []string{"grpc-go/1.48.0"},
	"TE":           []string{"trailers"},
}

type Client struct {
	ctx        context.Context
	serverAddr M.Socksaddr
	transport  *http2.Transport
	options    option.V2RayGRPCOptions
	url        *url.URL
	host       string
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayGRPCOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
	var host string
	if tlsConfig != nil && tlsConfig.ServerName() != "" {
		host = M.ParseSocksaddrHostPort(tlsConfig.ServerName(), serverAddr.Port).String()
	} else {
		host = serverAddr.String()
	}

	client := &Client{
		ctx:        ctx,
		serverAddr: serverAddr,
		options:    options,
		transport: &http2.Transport{
			ReadIdleTimeout:    time.Duration(options.IdleTimeout),
			PingTimeout:        time.Duration(options.PingTimeout),
			DisableCompression: true,
		},
		url: &url.URL{
			Scheme:  "https",
			Host:    serverAddr.String(),
			Path:    "/" + options.ServiceName + "/Tun",
			RawPath: "/" + url.PathEscape(options.ServiceName) + "/Tun",
		},
		host: host,
	}

	setupDial := func(transport *http2.Transport) {
		if tlsConfig == nil {
			transport.DialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
				return dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
			}
			return
		}
		if len(tlsConfig.NextProtos()) == 0 {
			tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
		}
		tlsDialer := tls.NewDialer(dialer, tlsConfig)
		transport.DialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
			return tlsDialer.DialTLSContext(ctx, M.ParseSocksaddr(addr))
		}
	}

	setupDial(client.transport)

	if options.Pool == nil || !options.Pool.Enabled || options.ForceLite {
		return client, nil
	}

	poolTransport := &http2.Transport{
		ReadIdleTimeout:    time.Duration(options.IdleTimeout),
		PingTimeout:        time.Duration(options.PingTimeout),
		DisableCompression: true,
	}
	setupDial(poolTransport)

	cfg := poolConfig{ //nolint:exhaustruct
		maxConnections:     options.Pool.MaxConnections,
		maxStreams:         options.Pool.MaxStreams,
		maxConnecting:      options.Pool.MaxConnecting,
		maxReuse:           options.Pool.MaxReuse,
		maxAge:             time.Duration(options.Pool.MaxAge),
		waitTimeout:        time.Duration(options.Pool.WaitTimeout),
		minConnections:     options.Pool.MinConnections,
		failureBackoff:     time.Duration(options.Pool.FailureBackoff),
		maxFailureBackoff:  time.Duration(options.Pool.MaxFailureBackoff),
		restoreStableAfter: time.Duration(options.Pool.RestoreStableAfter),
	}
	if cfg.maxStreams == 0 {
		cfg.maxStreams = 16
	}
	if cfg.maxConnecting == 0 {
		cfg.maxConnecting = 2
	}
	if cfg.waitTimeout == 0 {
		cfg.waitTimeout = 5 * time.Second
	}
	if cfg.failureBackoff == 0 {
		cfg.failureBackoff = time.Second
	}
	if cfg.maxFailureBackoff == 0 {
		cfg.maxFailureBackoff = 16 * time.Second
	}
	if cfg.restoreStableAfter == 0 {
		cfg.restoreStableAfter = 3 * time.Minute
	}
	if cfg.failureBackoff < 0 {
		return nil, E.New("v2ray-grpc: pool.failure_backoff must be positive")
	}
	if cfg.maxFailureBackoff < cfg.failureBackoff {
		return nil, E.New("v2ray-grpc: pool.max_failure_backoff must be greater than or equal to failure_backoff")
	}
	if cfg.restoreStableAfter < 0 {
		return nil, E.New("v2ray-grpc: pool.restore_stable_after must be positive")
	}

	factory := func(ctx context.Context) (clientConn, error) {
		rawConn, err := poolTransport.DialTLSContext(ctx, N.NetworkTCP, serverAddr.String(), nil)
		if err != nil {
			return nil, err
		}
		cc, err := poolTransport.NewClientConn(rawConn)
		if err != nil {
			_ = rawConn.Close()
			return nil, err
		}
		return cc, nil
	}

	makePool := func() poolAcquirer {
		return newPool(ctx, cfg, factory)
	}

	return &pooledClient{
		base:     client,
		pool:     makePool(),
		makePool: makePool,
		url:      client.url,
		host:     client.host,
	}, nil
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
	go func() {
		response, err := c.transport.RoundTrip(request)
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
	v2rayhttp.ResetTransport(c.transport)
	return nil
}

type poolAcquirer interface {
	Acquire(ctx context.Context) (clientConn, func(), error)
	Close() error
}

type clientTransport interface {
	adapter.V2RayClientTransport
}

type pooledGunConn struct {
	*GunConn

	releaseOnce sync.Once
	releaseMu   sync.Mutex
	release     func()
}

func newPooledGunConn(writer io.Writer) *pooledGunConn {
	return &pooledGunConn{GunConn: newLateGunConn(writer)} //nolint:exhaustruct
}

func (c *pooledGunConn) track(clientConn clientConn) {
	c.GunConn.track(clientConn)
}

func (c *pooledGunConn) markBroken(err error) {
	c.GunConn.markBroken(err)
}

func (c *pooledGunConn) setRelease(release func()) {
	c.releaseMu.Lock()
	c.release = release
	c.releaseMu.Unlock()
}

func (c *pooledGunConn) doRelease() {
	c.releaseOnce.Do(func() {
		c.releaseMu.Lock()
		release := c.release
		c.release = nil
		c.releaseMu.Unlock()
		if release != nil {
			release()
		}
	})
}

func (c *pooledGunConn) Close() error {
	c.doRelease()
	return c.GunConn.Close()
}

type pooledClient struct {
	base     clientTransport
	poolMu   sync.Mutex
	pool     poolAcquirer
	makePool func() poolAcquirer
	url      *url.URL
	host     string
}

func (c *pooledClient) getPool() poolAcquirer {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	if c.pool == nil && c.makePool != nil {
		c.pool = c.makePool()
	}
	return c.pool
}

func (c *pooledClient) resetPool() {
	c.poolMu.Lock()
	old := c.pool
	c.pool = nil
	c.poolMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

func (c *pooledClient) DialContext(ctx context.Context) (net.Conn, error) {
	if adapter.IsURLTestFromContext(ctx) {
		log.TraceContext(ctx, "v2raygrpclite: bypass pool for urltest/delay")
		return c.base.DialContext(ctx)
	}

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

	conn := newPooledGunConn(pipeInWriter)
	conn.setCancel(cancel)
	go func() {
		pool := c.getPool()
		clientConn, release, err := pool.Acquire(ctx)
		if errors.Is(err, context.Canceled) && ctx.Err() == nil {
			c.resetPool()
			pool = c.getPool()
			clientConn, release, err = pool.Acquire(ctx)
		}
		if err != nil {
			_ = pipeInWriter.CloseWithError(err)
			conn.setup(nil, err)
			return
		}
		conn.setRelease(release)
		response, err := clientConn.RoundTrip(request)
		if err != nil {
			_ = pipeInWriter.CloseWithError(err)
			conn.setup(nil, err)
			conn.doRelease()
		} else if response.StatusCode != 200 {
			response.Body.Close()
			err = E.New("v2ray-grpc: unexpected status: ", response.Status)
			conn.markBroken(err)
			_ = pipeInWriter.CloseWithError(err)
			conn.setup(nil, err)
			conn.doRelease()
		} else {
			conn.track(clientConn)
			conn.setup(response.Body, nil)
		}
	}()
	return conn, nil
}

func (c *pooledClient) Close() error {
	c.resetPool()
	return c.base.Close()
}
