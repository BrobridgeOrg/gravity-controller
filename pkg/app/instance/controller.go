package instance

import (
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller"
)

func (a *AppInstance) initController() error {
	return a.controller.Init()
}

func (a *AppInstance) GetController() controller.Controller {
	return controller.Controller(a.controller)
}
