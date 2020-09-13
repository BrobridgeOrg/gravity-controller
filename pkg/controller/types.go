package controller

type Controller interface {
	Init() error
	Register(string) error
	Unregister(string) error
	GetClientCount() uint64
	GetPipelineCount() uint64
	ReleasePipeline(string, uint64) error
}
