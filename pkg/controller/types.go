package controller

import (
	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
)

type SubscriberClient interface {
}

type Controller interface {
	Init() error

	// Core
	Register(string) error
	Unregister(string) error
	GetClientCount() uint64
	GetPipelineCount() uint64
	ReleasePipeline(string, uint64) error
	GetPipelines(string) ([]uint64, error)

	// Adapter
	RegisterAdapter(string) error
	UnregisterAdapter(string) error

	// Subscriber
	RegisterSubscriber(subscriber_manager_pb.SubscriberType, string, string, string) (SubscriberClient, error)
	UnregisterSubscriber(string) error

	Resync(string) error
}
