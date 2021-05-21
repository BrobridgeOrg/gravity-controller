package controller

type Adapter struct {
	controller *Controller
	id         string
	name       string
	component  string
}

func NewAdapter(controller *Controller, component string, id string, name string) *Adapter {
	return &Adapter{
		controller: controller,
		id:         id,
		name:       name,
		component:  component,
	}
}
