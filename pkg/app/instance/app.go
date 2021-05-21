package instance

import (
	controller_service "github.com/BrobridgeOrg/gravity-controller/pkg/controller/service"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done       chan bool
	controller *controller_service.Controller
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	a.controller = controller_service.NewController(a)

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	err := a.initController()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	<-a.done

	return nil
}
