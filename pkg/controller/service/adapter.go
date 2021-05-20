package controller

type Adapter struct {
	controller *Controller
	id         string
	pipelines  []uint64
}

func NewAdapter(controller *Controller, id string) *Adapter {
	return &Adapter{
		controller: controller,
		id:         id,
	}
}
