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
	synchronizerManager *SynchronizerManager
	pipelineManager     *PipelineManager
	adapterClients      map[string]*AdapterClient
	subscriberClients   map[string]*SubscriberClient

	mutex sync.RWMutex
}

func NewController(a app.App) *Controller {
	controller := &Controller{
		app:               a,
		gravityClient:     core.NewClient(),
		adapterClients:    make(map[string]*AdapterClient),
		subscriberClients: make(map[string]*SubscriberClient),
	}

	controller.synchronizerManager = NewSynchronizerManager(controller)
	controller.pipelineManager = NewPipelineManager(controller)

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

	// Initializing synchronizer manager
	err = controller.synchronizerManager.Initialize()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Initializing RPC handlers
	err = controller.InitRPCHandlers()
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

func (controller *Controller) RegisterAdapter(synchronizerID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	_, ok := controller.adapterClients[synchronizerID]
	if ok {
		return nil
	}

	// Create a new synchronizer
	synchronizer := NewAdapterClient(controller, synchronizerID)
	controller.adapterClients[synchronizerID] = synchronizer

	log.WithFields(log.Fields{
		"synchronizerID": synchronizerID,
	}).Info("Registered Adapter synchronizer")

	return nil
}

func (controller *Controller) UnregisterAdapter(synchronizerID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	// Take off synchronizer from registry
	delete(controller.adapterClients, synchronizerID)

	return nil
}

func (controller *Controller) Resync(destinationName string) error {

	//TODO: Resync for specific destination

	return nil
}
