package rpc

import "crypto/tls"

type ProviderFactory = func(instanceID string) Provider

func NewRpcProviderFactory(tlsConf *tls.Config) ProviderFactory {
	return func(instanceID string) Provider {
		return NewRpcProvider(tlsConf, instanceID)
	}
}
