package controller

type SubscriberClient interface {
	GetChannel() uint64
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
	RegisterSubscriber(string) (SubscriberClient, error)
	UnregisterSubscriber(string) error

	Resync(string) error
}
