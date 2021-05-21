package controller

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
}
