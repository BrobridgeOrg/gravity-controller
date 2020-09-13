package controller

type Pipeline struct {
	id       uint64
	clientID string
}

func NewPipeline(id uint64) *Pipeline {
	return &Pipeline{
		id: id,
	}
}

func (pipeline *Pipeline) Assign(clientID string) {
	pipeline.clientID = clientID
}

func (pipeline *Pipeline) Release() {
	pipeline.clientID = ""
}
