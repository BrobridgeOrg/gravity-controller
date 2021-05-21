package controller

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-controller/pkg/app"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	log "github.com/sirupsen/logrus"
)

type Controller struct {
	app                 app.App
	gravityClient       *core.Client
	clientID            string
	adapterManager      *AdapterManager
	synchronizerManager *SynchronizerManager
	pipelineManager     *PipelineManager
	subscriberManager   *SubscriberManager

	mutex sync.RWMutex
}

func NewController(a app.App) *Controller {
	controller := &Controller{
		app:           a,
		gravityClient: core.NewClient(),
	}

	controller.adapterManager = NewAdapterManager(controller)
	controller.synchronizerManager = NewSynchronizerManager(controller)
	controller.pipelineManager = NewPipelineManager(controller)
	controller.subscriberManager = NewSubscriberManager(controller)

	return controller
}

func (controller *Controller) Init() error {

	// Using hostname (pod name) by default
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		return nil
	}

	host = strings.ReplaceAll(host, ".", "_")

	controller.clientID = fmt.Sprintf("gravity_controller-%s", host)

	// Initializing gravity
	err = controller.initializeClient()
	if err != nil {
		log.Error(err)
		timer := time.NewTimer(1000 * time.Millisecond)
		<-timer.C
		return controller.Init()
	}

	// Initializing pipeline manager
	err = controller.pipelineManager.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Initializing adapter manager
	err = controller.adapterManager.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Initializing synchronizer manager
	err = controller.synchronizerManager.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Initializing subscriber manager
	err = controller.subscriberManager.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	return nil
}

func (controller *Controller) DispatchPipeline(pipeline *Pipeline) bool {
	return controller.pipelineManager.DispatchPipeline(pipeline)
}

func (controller *Controller) Register(synchronizerID string) error {
	return controller.synchronizerManager.Register(synchronizerID)
}

func (controller *Controller) Unregister(synchronizerID string) error {
	return controller.synchronizerManager.Unregister(synchronizerID)
}

func (controller *Controller) GetClientCount() uint64 {
	return uint64(controller.synchronizerManager.GetCount())
}

func (controller *Controller) GetPipelineCount() uint64 {
	return uint64(controller.pipelineManager.GetCount())
}

func (controller *Controller) ReleasePipeline(synchronizerID string, pipelineID uint64) error {
	return controller.pipelineManager.ReleasePipeline(synchronizerID, pipelineID)
}

func (controller *Controller) AssignPipeline(synchronizerID string, pipelineID uint64) error {
	return controller.pipelineManager.AssignPipeline(synchronizerID, pipelineID)
}

func (controller *Controller) RevokePipeline(synchronizerID string, pipelineID uint64) error {
	return controller.pipelineManager.RevokePipeline(synchronizerID, pipelineID)
}

func (controller *Controller) GetPipelines(synchronizerID string) ([]uint64, error) {

	synchronizer := controller.synchronizerManager.GetSynchronizer(synchronizerID)
	if synchronizer == nil {
		return nil, errors.New("No such synchronizer: " + synchronizerID)
	}

	return synchronizer.pipelines, nil
}
