// Copyright 2023-2026 The Oxia Authors
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

package security

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	libtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateSelfSignedCert(t *testing.T, cn string) (certPEM []byte, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0600))
	return path
}

func TestTrustedCertPool_MultipleCerts(t *testing.T) {
	dir := t.TempDir()

	cert1PEM, _ := generateSelfSignedCert(t, "CA1")
	cert2PEM, _ := generateSelfSignedCert(t, "CA2")

	// Bundle both certs into one PEM file
	bundlePath := writeTempFile(t, dir, "ca-bundle.pem", append(cert1PEM, cert2PEM...))

	opts := &TLSOptions{TrustedCaFile: bundlePath}
	pool, err := opts.trustedCertPool()
	require.NoError(t, err)

	// The pool should contain both certificates
	subjects := pool.Subjects() //nolint:staticcheck
	assert.Len(t, subjects, 2)
}

func TestTrustedCertPool_SingleCert(t *testing.T) {
	dir := t.TempDir()

	certPEM, _ := generateSelfSignedCert(t, "SingleCA")
	caPath := writeTempFile(t, dir, "ca.pem", certPEM)

	opts := &TLSOptions{TrustedCaFile: caPath}
	pool, err := opts.trustedCertPool()
	require.NoError(t, err)

	subjects := pool.Subjects() //nolint:staticcheck
	assert.Len(t, subjects, 1)
}

func TestTrustedCertPool_NoCerts(t *testing.T) {
	dir := t.TempDir()

	// Write a PEM file with only a private key block (no CERTIFICATE)
	keyBlock := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte("fake")})
	keyPath := writeTempFile(t, dir, "no-certs.pem", keyBlock)

	opts := &TLSOptions{TrustedCaFile: keyPath}
	_, err := opts.trustedCertPool()
	assert.ErrorContains(t, err, "no valid certificates found")
}

func TestTrustedCertPool_InvalidCert(t *testing.T) {
	dir := t.TempDir()

	// Write a PEM file with an invalid CERTIFICATE block
	badCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a valid cert")})
	badPath := writeTempFile(t, dir, "bad-cert.pem", badCert)

	opts := &TLSOptions{TrustedCaFile: badPath}
	_, err := opts.trustedCertPool()
	assert.ErrorContains(t, err, "failed to parse certificate")
}

func TestTrustedCertPool_FileNotFound(t *testing.T) {
	opts := &TLSOptions{TrustedCaFile: "/nonexistent/path/ca.pem"}
	_, err := opts.trustedCertPool()
	assert.Error(t, err)
}

func TestMakeCommonConfig_MinVersionBelowTLS12(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedCert(t, "test")
	certPath := writeTempFile(t, dir, "cert.pem", certPEM)
	keyPath := writeTempFile(t, dir, "key.pem", keyPEM)

	opts := &TLSOptions{
		CertFile:   certPath,
		KeyFile:    keyPath,
		MinVersion: libtls.VersionTLS10,
	}
	_, err := opts.makeCommonConfig()
	assert.ErrorContains(t, err, "below TLS 1.2")
}

func TestMakeCommonConfig_MaxVersionBelowMin(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedCert(t, "test")
	certPath := writeTempFile(t, dir, "cert.pem", certPEM)
	keyPath := writeTempFile(t, dir, "key.pem", keyPEM)

	opts := &TLSOptions{
		CertFile:   certPath,
		KeyFile:    keyPath,
		MinVersion: libtls.VersionTLS13,
		MaxVersion: libtls.VersionTLS12,
	}
	_, err := opts.makeCommonConfig()
	assert.ErrorContains(t, err, "below minimum")
}

func TestMakeCommonConfig_ValidVersions(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedCert(t, "test")
	certPath := writeTempFile(t, dir, "cert.pem", certPEM)
	keyPath := writeTempFile(t, dir, "key.pem", keyPEM)

	opts := &TLSOptions{
		CertFile:   certPath,
		KeyFile:    keyPath,
		MinVersion: libtls.VersionTLS12,
		MaxVersion: libtls.VersionTLS13,
	}
	conf, err := opts.makeCommonConfig()
	require.NoError(t, err)
	assert.Equal(t, uint16(libtls.VersionTLS12), conf.MinVersion)
	assert.Equal(t, uint16(libtls.VersionTLS13), conf.MaxVersion)
}

func TestTryIntoServerTLSConf_ClientAuthTrust(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedCert(t, "server")
	certPath := writeTempFile(t, dir, "cert.pem", certPEM)
	keyPath := writeTempFile(t, dir, "key.pem", keyPEM)
	caPEM, _ := generateSelfSignedCert(t, "ClientCA")
	caPath := writeTempFile(t, dir, "ca.pem", caPEM)

	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logOutput, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	disabled := false
	tests := []struct {
		name           string
		enabled        *bool
		clientAuth     bool
		trustedCaFile  string
		wantClientAuth libtls.ClientAuthType
		wantWarning    bool
	}{
		{
			name:           "required client authentication with system roots",
			clientAuth:     true,
			wantClientAuth: libtls.RequireAndVerifyClientCert,
			wantWarning:    true,
		},
		{
			name:           "required client authentication with explicit CA",
			clientAuth:     true,
			trustedCaFile:  caPath,
			wantClientAuth: libtls.RequireAndVerifyClientCert,
		},
		{
			name:           "no client authentication",
			wantClientAuth: libtls.NoClientCert,
		},
		{
			name:           "optional client authentication with explicit CA",
			trustedCaFile:  caPath,
			wantClientAuth: libtls.VerifyClientCertIfGiven,
		},
		{
			name:       "TLS disabled",
			enabled:    &disabled,
			clientAuth: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput.Reset()
			opts := &TLSOptions{
				Enabled:       tt.enabled,
				CertFile:      certPath,
				KeyFile:       keyPath,
				TrustedCaFile: tt.trustedCaFile,
				ClientAuth:    tt.clientAuth,
			}
			conf, err := opts.TryIntoServerTLSConf()
			require.NoError(t, err)
			if tt.wantWarning {
				assert.Contains(t, logOutput.String(), "level=WARN")
				assert.Contains(t, logOutput.String(), "using system roots")
				assert.Contains(t, logOutput.String(), "trustedCaFile")
			} else {
				assert.Empty(t, logOutput.String())
			}
			if !opts.IsEnabled() {
				assert.Nil(t, conf)
				return
			}
			require.NotNil(t, conf)
			assert.Equal(t, tt.wantClientAuth, conf.ClientAuth)
			if tt.trustedCaFile == "" {
				assert.Nil(t, conf.ClientCAs)
			} else {
				require.NotNil(t, conf.ClientCAs)
				assert.Len(t, conf.ClientCAs.Subjects(), 1) //nolint:staticcheck
			}
		})
	}
}
