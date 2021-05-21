package controller

type Pipeline struct {
	id             uint64
	synchronizerID string
}

func NewPipeline(id uint64) *Pipeline {
	return &Pipeline{
		id: id,
	}
}

func (pipeline *Pipeline) Assign(synchronizerID string) {
	pipeline.synchronizerID = synchronizerID
}

func (pipeline *Pipeline) Release() {
	pipeline.synchronizerID = ""
}
