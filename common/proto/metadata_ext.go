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

package proto

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultLoadBalancerScheduleInterval = 30 * time.Second
	defaultLoadBalancerQuarantineTime   = 5 * time.Minute
)

func (ds *DataServer) GetIdentifier() string {
	if ds == nil {
		return ""
	}
	if ds.Name != nil {
		return ds.GetName()
	}
	return ds.GetInternalAddress()
}

func (ns *Namespace) NotificationsEnabledOrDefault() bool {
	if ns == nil || ns.NotificationsEnabled == nil {
		return true
	}
	return ns.GetNotificationsEnabled()
}

func (cc *ClusterConfiguration) GetDataServerInfo(id string) (*DataServerInfo, bool) {
	if cc == nil {
		return nil, false
	}

	for _, server := range cc.GetServers() {
		if server.GetIdentifier() != id {
			continue
		}

		dataServer := server
		if server.GetName() == "" {
			name := server.GetIdentifier()
			dataServer = &DataServer{
				Name:            &name,
				PublicAddress:   server.GetPublicAddress(),
				InternalAddress: server.GetInternalAddress(),
			}
		}

		info := &DataServerInfo{
			DataServer: dataServer,
			Metadata:   &DataServerMetadata{},
		}
		if metadata, found := cc.GetServerMetadata()[id]; found {
			info.Metadata = metadata
		}
		return info, true
	}

	return nil, false
}

func (cc *ClusterConfiguration) LoadBalancerWithDefaults() *LoadBalancer {
	loadBalancer := &LoadBalancer{
		ScheduleInterval: durationpb.New(defaultLoadBalancerScheduleInterval),
		QuarantineTime:   durationpb.New(defaultLoadBalancerQuarantineTime),
	}

	if cc == nil || cc.GetLoadBalancer() == nil {
		return loadBalancer
	}

	if duration := cc.GetLoadBalancer().GetScheduleInterval().AsDuration(); duration != 0 {
		loadBalancer.ScheduleInterval = durationpb.New(duration)
	}
	if duration := cc.GetLoadBalancer().GetQuarantineTime().AsDuration(); duration != 0 {
		loadBalancer.QuarantineTime = durationpb.New(duration)
	}
	return loadBalancer
}
