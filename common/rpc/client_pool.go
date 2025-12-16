// Copyright 2023-2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/auth"
)

const DefaultRpcTimeout = 30 * time.Second
const AddressSchemaTLS = "tls://"
const (
	defaultDialTimeout                   = time.Second * 5
	defaultGrpcClientKeepAliveTime       = time.Second * 10
	defaultGrpcClientKeepAliveTimeout    = time.Second * 5
	defaultGrpcClientPermitWithoutStream = true
)

type ClientPool interface {
	io.Closer
	GetClientRpc(target string) (proto.OxiaClientClient, error)
	GetHealthRpc(target string) (grpc_health_v1.HealthClient, io.Closer, error)
	GetCoordinationRpc(target string) (proto.OxiaCoordinationClient, error)
	GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error)
	GetAminRpc(target string) (proto.OxiaAdminClient, error)

	// Clear all the pooled client instances for the given target
	Clear(target string)
}

type ClientPoolOptions struct {
	TLS            *tls.Config
	Authentication auth.Authentication
	DisableIPv6    bool
}

type clientPool struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn

	tls            *tls.Config
	authentication auth.Authentication
	disableIPv6    bool
	log            *slog.Logger
}

func (cp *clientPool) GetAminRpc(target string) (proto.OxiaAdminClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}
	return proto.NewOxiaAdminClient(cnx), nil
}

func NewClientPool(tlsConf *tls.Config, authentication auth.Authentication) ClientPool {
	return NewClientPoolWithOptions(ClientPoolOptions{
		TLS:            tlsConf,
		Authentication: authentication,
		DisableIPv6:    false, // Default: allow IPv6
	})
}

func NewClientPoolWithOptions(opts ClientPoolOptions) ClientPool {
	return &clientPool{
		connections:    make(map[string]*grpc.ClientConn),
		tls:            opts.TLS,
		authentication: opts.Authentication,
		disableIPv6:    opts.DisableIPv6,
		log: slog.With(
			slog.String("component", "client-pool"),
		),
	}
}

func (cp *clientPool) Close() error {
	cp.Lock()
	defer cp.Unlock()

	for target, cnx := range cp.connections {
		err := cnx.Close()
		if err != nil {
			cp.log.Warn(
				"Failed to close GRPC connection",
				slog.String("server_address", target),
				slog.Any("error", err),
			)
		}
	}
	return nil
}

func (cp *clientPool) GetHealthRpc(target string) (grpc_health_v1.HealthClient, io.Closer, error) {
	// Skip the pooling for health-checks
	cnx, err := cp.newConnection(target)
	if err != nil {
		return nil, nil, err
	}

	return grpc_health_v1.NewHealthClient(cnx), cnx, nil
}

func (cp *clientPool) GetClientRpc(target string) (proto.OxiaClientClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return &loggingClientRpc{target, proto.NewOxiaClientClient(cnx)}, nil
}

func (cp *clientPool) GetCoordinationRpc(target string) (proto.OxiaCoordinationClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return proto.NewOxiaCoordinationClient(cnx), nil
}

func (cp *clientPool) GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return proto.NewOxiaLogReplicationClient(cnx), nil
}

func (cp *clientPool) Clear(target string) {
	cp.Lock()
	defer cp.Unlock()

	if cnx, ok := cp.connections[target]; ok {
		if err := cnx.Close(); err != nil {
			cp.log.Warn(
				"Failed to close GRPC connection",
				slog.String("server_address", target),
				slog.Any("error", err),
			)
		}

		delete(cp.connections, target)
	}
}

func (cp *clientPool) getConnectionFromPool(target string) (grpc.ClientConnInterface, error) {
	cp.RLock()
	cnx, ok := cp.connections[target]
	cp.RUnlock()
	if ok {
		return cnx, nil
	}

	cp.Lock()
	defer cp.Unlock()

	cnx, ok = cp.connections[target]
	if ok {
		return cnx, nil
	}

	cnx, err := cp.newConnection(target)
	if err != nil {
		return nil, err
	}
	cp.connections[target] = cnx
	return cnx, nil
}

func (cp *clientPool) newConnection(target string) (*grpc.ClientConn, error) {
	cp.log.Debug(
		"Creating new GRPC connection",
		slog.String("server_address", target),
	)

	tcs := cp.getTransportCredential(target)

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(tcs),
		grpc.WithStreamInterceptor(grpcprometheus.StreamClientInterceptor),
		grpc.WithUnaryInterceptor(grpcprometheus.UnaryClientInterceptor),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			PermitWithoutStream: defaultGrpcClientPermitWithoutStream,
			Time:                defaultGrpcClientKeepAliveTime,
			Timeout:             defaultGrpcClientKeepAliveTimeout,
		}),
		grpc.WithContextDialer(cp.createDialer()),
	}
	if cp.authentication != nil {
		options = append(options, grpc.WithPerRPCCredentials(cp.authentication))
	}
	cnx, err := grpc.NewClient(cp.getActualAddress(target), options...)
	if err != nil {
		return nil, errors.Wrapf(err, "error connecting to %s", target)
	}

	return cnx, nil
}

func (cp *clientPool) createDialer() func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		ips, err := cp.resolveIPs(ctx, host)
		if err != nil {
			return nil, err
		}

		ips = cp.sortIPs(ips)

		return cp.dialIPs(ctx, ips, port, host)
	}
}

func (cp *clientPool) resolveIPs(ctx context.Context, host string) ([]net.IP, error) {
	var ips []net.IP
	var err error

	if cp.disableIPv6 {
		cp.log.Debug("Resolving hostname to IPv4 (IPv6 disabled)", slog.String("host", host))
		ips, err = net.DefaultResolver.LookupIP(ctx, "ip4", host)
	} else {
		cp.log.Debug("Resolving hostname (IPv4 and IPv6)", slog.String("host", host))
		ips, err = net.DefaultResolver.LookupIP(ctx, "ip", host)
	}

	if err != nil {
		cp.log.Debug("DNS resolution failed", slog.String("host", host), slog.Any("error", err))
		return nil, err
	}
	if len(ips) == 0 {
		networkType := "IP"
		if cp.disableIPv6 {
			networkType = "IPv4"
		}
		cp.log.Debug("No addresses found", slog.String("host", host), slog.String("networkType", networkType))
		return nil, errors.Errorf("no %s address found for %s", networkType, host)
	}

	ipStrings := make([]string, len(ips))
	for i, ip := range ips {
		ipStrings[i] = ip.String()
	}
	cp.log.Debug("DNS resolution successful",
		slog.String("host", host),
		slog.Any("resolvedIPs", ipStrings),
	)

	return ips, nil
}

func (cp *clientPool) sortIPs(ips []net.IP) []net.IP {
	if cp.disableIPv6 {
		return ips
	}

	ipv4s := make([]net.IP, 0)
	ipv6s := make([]net.IP, 0)
	for _, ip := range ips {
		if ip.To4() != nil {
			ipv4s = append(ipv4s, ip)
		} else {
			ipv6s = append(ipv6s, ip)
		}
	}
	sortedIPs := make([]net.IP, 0, len(ipv4s)+len(ipv6s))
	sortedIPs = append(sortedIPs, ipv4s...)
	sortedIPs = append(sortedIPs, ipv6s...)
	if len(ipv4s) > 0 && len(ipv6s) > 0 {
		cp.log.Debug("Preferring IPv4 addresses", slog.Int("ipv4Count", len(ipv4s)), slog.Int("ipv6Count", len(ipv6s)))
	}
	return sortedIPs
}

func (cp *clientPool) dialIPs(ctx context.Context, ips []net.IP, port, host string) (net.Conn, error) {
	d := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: defaultGrpcClientKeepAliveTime,
	}
	var lastErr error
	for _, ip := range ips {
		network := "tcp4"
		if ip.To4() == nil {
			network = "tcp6"
		}
		ipAddr := net.JoinHostPort(ip.String(), port)
		cp.log.Debug("Attempting connection", slog.String("ip", ip.String()), slog.String("network", network))
		conn, err := d.DialContext(ctx, network, ipAddr)
		if err == nil {
			cp.log.Debug("Connection successful", slog.String("ip", ip.String()))
			return conn, nil
		}
		cp.log.Debug("Connection failed, trying next IP", slog.String("ip", ip.String()), slog.Any("error", err))
		lastErr = err
	}

	cp.log.Debug("All connection attempts failed", slog.String("host", host), slog.Any("error", lastErr))
	return nil, errors.Wrapf(lastErr, "failed to connect to any resolved IP for %s", host)
}

func (*clientPool) getActualAddress(target string) string {
	addr := target
	if strings.HasPrefix(target, AddressSchemaTLS) {
		addr, _ = strings.CutPrefix(target, AddressSchemaTLS)
	}
	// Use passthrough to skip gRPC's DNS resolver, we do our own IPv4-only resolution in the dialer
	if !strings.Contains(addr, "://") {
		addr = "passthrough:///" + addr
	}
	return addr
}

func (cp *clientPool) getTransportCredential(target string) credentials.TransportCredentials {
	// Use TLS if address has tls:// prefix OR if TLS config is provided
	useTLS := strings.HasPrefix(target, AddressSchemaTLS) || cp.tls != nil

	if !useTLS {
		return insecure.NewCredentials()
	}

	hostname := GetHostname(target)
	tlsConfig := cp.buildTLSConfig(hostname)
	return credentials.NewTLS(tlsConfig)
}

func (cp *clientPool) buildTLSConfig(hostname string) *tls.Config {
	tlsConfig := &tls.Config{
		ServerName: hostname, // Go verifies hostname automatically
		MinVersion: tls.VersionTLS12,
	}

	cp.setRootCAs(tlsConfig, hostname)
	cp.mergeCustomTLSConfig(tlsConfig)
	cp.setupVerifyConnection(tlsConfig)

	return tlsConfig
}

func (cp *clientPool) setRootCAs(tlsConfig *tls.Config, hostname string) {
	if cp.tls != nil && cp.tls.RootCAs != nil {
		cp.log.Debug("Using custom RootCAs from TLS config")
		tlsConfig.RootCAs = cp.tls.RootCAs
		return
	}

	cp.log.Debug("Loading system certificate pool")
	roots, err := x509.SystemCertPool()
	if err != nil {
		cp.log.Warn("Failed to load system cert pool, creating new one", slog.Any("error", err))
		roots = x509.NewCertPool()
		tlsConfig.RootCAs = roots
		return
	}

	cp.log.Debug("System certificate pool loaded successfully",
		slog.String("hostname", hostname),
		slog.Bool("usingSystemCertPool", true),
	)
	tlsConfig.RootCAs = roots
}

func (cp *clientPool) mergeCustomTLSConfig(tlsConfig *tls.Config) {
	if cp.tls == nil {
		return
	}

	if cp.tls.ServerName != "" {
		tlsConfig.ServerName = cp.tls.ServerName
	}
	if cp.tls.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}
	if cp.tls.Certificates != nil {
		tlsConfig.Certificates = cp.tls.Certificates
	}
}

func (cp *clientPool) setupVerifyConnection(tlsConfig *tls.Config) {
	if tlsConfig.InsecureSkipVerify {
		return
	}

	originalVerifyConnection := tlsConfig.VerifyConnection
	tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
		cp.logCertificateInfo(cs, tlsConfig)
		return cp.verifyCertificate(cs, originalVerifyConnection)
	}
}

func (cp *clientPool) logCertificateInfo(cs tls.ConnectionState, tlsConfig *tls.Config) {
	if len(cs.PeerCertificates) == 0 {
		return
	}

	cert := cs.PeerCertificates[0]
	cp.log.Debug("TLS handshake completed, verifying certificate",
		slog.String("subject", cert.Subject.String()),
		slog.String("issuer", cert.Issuer.String()),
		slog.String("commonName", cert.Subject.CommonName),
		slog.Any("dnsNames", cert.DNSNames),
		slog.String("serverName", tlsConfig.ServerName),
		slog.Int("peerCertificates", len(cs.PeerCertificates)),
		slog.Bool("hasRootCAs", tlsConfig.RootCAs != nil),
	)

	if len(cs.PeerCertificates) > 1 {
		intermediates := make([]string, len(cs.PeerCertificates)-1)
		for i, icert := range cs.PeerCertificates[1:] {
			intermediates[i] = fmt.Sprintf("issuer=%s,subject=%s", icert.Issuer.String(), icert.Subject.String())
		}
		cp.log.Debug("Intermediate certificates in chain", slog.Any("intermediates", intermediates))
	}
}

func (cp *clientPool) verifyCertificate(cs tls.ConnectionState, originalVerifyConnection func(tls.ConnectionState) error) error {
	if originalVerifyConnection == nil {
		return nil
	}

	err := originalVerifyConnection(cs)
	if err != nil {
		cp.log.Debug("Certificate verification failed",
			slog.Any("error", err),
			slog.String("errorType", fmt.Sprintf("%T", err)),
		)
	} else {
		cp.log.Debug("Certificate verification succeeded")
	}
	return err
}

func GetHostname(target string) string {
	addr := target
	if strings.HasPrefix(target, AddressSchemaTLS) {
		addr, _ = strings.CutPrefix(target, AddressSchemaTLS)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// If no port, use the address as-is
		return addr
	}
	return host
}

func GetPeer(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	return p.Addr.String()
}
