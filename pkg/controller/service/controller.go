package controller

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-controller/pkg/app"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Controller struct {
	app               app.App
	eventBus          *EventBus
	clientID          string
	clients           map[string]*Client
	adapterClients    map[string]*AdapterClient
	subscriberClients map[string]*SubscriberClient
	pipelines         map[uint64]*Pipeline

	pendingTasks chan *Task
	mutex        sync.RWMutex

	channelCounter uint64
}

func NewController(a app.App) *Controller {
	controller := &Controller{
		app:               a,
		clients:           make(map[string]*Client),
		adapterClients:    make(map[string]*AdapterClient),
		subscriberClients: make(map[string]*SubscriberClient),
		pipelines:         make(map[uint64]*Pipeline),
		channelCounter:    0,
	}

	controller.eventBus = NewEventBus(controller)

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

	// Initializing pipelines
	err = controller.InitPipeline()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Initializing eventbus
	err = controller.eventBus.Initialize()
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

func (controller *Controller) watchTasks() {

	for {
		select {
		case task := <-controller.pendingTasks:
			success := controller.HandleTask(task)
			if !success {
				// Failed to process this task so we handle it later
				controller.pendingTasks <- task
			}
		}
	}
}

func (controller *Controller) InitPipeline() error {

	// Initializing pipelines
	viper.SetDefault("controller.pipelineCount", 256)
	pipelineCount := viper.GetUint64("controller.pipelineCount")
	controller.pendingTasks = make(chan *Task, pipelineCount)
	for i := uint64(0); i < pipelineCount; i++ {
		pipeline := NewPipeline(i)
		controller.pipelines[i] = pipeline
		controller.pendingTasks <- NewTask(nil, pipeline)
	}

	go controller.watchTasks()

	return nil
}

func (controller *Controller) HandleTask(task *Task) bool {

	if task.Client == nil {
		return controller.DispatchPipeline(task.Pipeline)
	}

	// Re-assign pipeline to specific client
	err := controller.assignPipeline(task.Client, task.Pipeline)
	if err != nil {
		return false
	}

	return true
}

func (controller *Controller) DispatchPipeline(pipeline *Pipeline) bool {

	var found *Client

	controller.mutex.RLock()
	clients := controller.clients
	controller.mutex.RUnlock()

	// Find a client to assign pipeline
	for _, client := range clients {
		if found == nil {
			found = client
			continue
		}

		if len(found.pipelines) > len(client.pipelines) {
			found = client
		}
	}

	if found == nil {
		return false
	}

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
		"client":   found.id,
	}).Info("Assigning pipeline")

	timer := time.NewTimer(10 * time.Millisecond)
	<-timer.C
	// Assign pipeline to client
	err := found.AssignPipeline(pipeline.id)
	if err != nil {
		return false
	}

	return true
}

func (controller *Controller) Register(clientID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	_, ok := controller.clients[clientID]
	if ok {
		return nil
	}

	// Create a new client
	client := NewClient(controller, clientID)
	controller.clients[clientID] = client

	log.WithFields(log.Fields{
		"clientID": clientID,
	}).Info("Registered client")

	return nil
}

func (controller *Controller) Unregister(clientID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	client, ok := controller.clients[clientID]
	if ok {
		return nil
	}

	// Release pipelines
	for _, pipelineID := range client.pipelines {

		// Getting pipeline by ID
		pipeline, ok := controller.pipelines[pipelineID]
		if !ok {
			return nil
		}

		controller.releasePipeline(client, pipeline)
	}

	// Take off client from registry
	delete(controller.clients, clientID)

	return nil
}

func (controller *Controller) GetClientCount() uint64 {
	return uint64(len(controller.clients))
}

func (controller *Controller) GetPipelineCount() uint64 {
	return uint64(len(controller.pipelines))
}

func (controller *Controller) releasePipeline(client *Client, pipeline *Pipeline) error {

	// Release pipeline back to pool
	pipeline.Release()
	controller.pendingTasks <- NewTask(nil, pipeline)

	return nil
}

func (controller *Controller) ReleasePipeline(clientID string, pipelineID uint64) error {

	client, ok := controller.clients[clientID]
	if !ok {
		return nil
	}

	if !client.ReleasePipeline(pipelineID) {
		return nil
	}

	pipeline, ok := controller.pipelines[pipelineID]
	if !ok {
		return nil
	}

	return controller.releasePipeline(client, pipeline)
}

func (controller *Controller) assignPipeline(client *Client, pipeline *Pipeline) error {

	err := client.AssignPipeline(pipeline.id)
	if err != nil {
		return err
	}

	pipeline.Assign(client.id)

	return nil
}

func (controller *Controller) AssignPipeline(clientID string, pipelineID uint64) error {

	client, ok := controller.clients[clientID]
	if !ok {
		return errors.New("No such client: " + clientID)
	}

	pipeline, ok := controller.pipelines[pipelineID]
	if !ok {
		return errors.New("No such pipeline: " + fmt.Sprintf("%d", pipelineID))
	}

	return controller.assignPipeline(client, pipeline)
}

func (controller *Controller) RevokePipeline(clientID string, pipelineID uint64) error {

	client, ok := controller.clients[clientID]
	if !ok {
		return errors.New("No such client: " + clientID)
	}

	pipeline, ok := controller.pipelines[pipelineID]
	if !ok {
		return errors.New("No such pipeline: " + fmt.Sprintf("%d", pipelineID))
	}

	return client.RevokePipeline(pipeline.id)
}

func (controller *Controller) GetPipelines(clientID string) ([]uint64, error) {

	client, ok := controller.clients[clientID]
	if !ok {
		return nil, errors.New("No such client: " + clientID)
	}

	return client.pipelines, nil
}

func (controller *Controller) RegisterAdapter(clientID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	_, ok := controller.adapterClients[clientID]
	if ok {
		return nil
	}

	// Create a new client
	client := NewAdapterClient(controller, clientID)
	controller.adapterClients[clientID] = client

	log.WithFields(log.Fields{
		"clientID": clientID,
	}).Info("Registered Adapter client")

	//TODO call synchronizer api to start send event

	return nil
}

func (controller *Controller) UnregisterAdapter(clientID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	/*
		client, ok := controller.adapterClients[clientID]
		if ok {
			return nil
		}

		//TODO call synchronizer api to stop send event
	*/

	// Take off client from registry
	delete(controller.adapterClients, clientID)

	return nil
}

func (controller *Controller) Resync(destinationName string) error {

	//TODO: Resync for specific destination

	return nil
}
