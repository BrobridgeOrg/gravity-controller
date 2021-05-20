package controller

import (
	"errors"
	"fmt"
	"sync"
	"time"

	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	def "github.com/BrobridgeOrg/gravity-controller/pkg/controller"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type SubscriberManager struct {
	controller  *Controller
	subscribers map[string]*SubscriberClient
	mutex       sync.RWMutex
}

func NewSubscriberManager(controller *Controller) *SubscriberManager {
	return &SubscriberManager{
		controller:  controller,
		subscribers: make(map[string]*SubscriberClient),
	}
}

func (sm *SubscriberManager) Initialize() error {

	err := sm.initialize_rpc()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SubscriberManager) register(eventstoreID string, subscriberID string) error {

	channel := fmt.Sprintf("gravity.eventstore.%s.registerSubscriber", eventstoreID)

	request := synchronizer_pb.RegisterSubscriberRequest{
		SubscriberID: subscriberID,
		Name:         subscriberID,
	}

	msg, _ := proto.Marshal(&request)

	conn := sm.controller.gravityClient.GetConnection()
	resp, err := conn.Request(channel, msg, time.Second*10)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.RegisterSubscriberReply
	err = proto.Unmarshal(resp.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		log.Error(reply.Reason)
		return err
	}

	return nil
}

func (sm *SubscriberManager) Register(subscriberType subscriber_manager_pb.SubscriberType, component string, subscriberID string, name string) (def.SubscriberClient, error) {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.subscribers[subscriberID]
	if ok {
		return nil, errors.New("Subscriber ID exists already")
	}

	// Create a new subscriber
	subscriber := NewSubscriberClient(sm.controller, subscriberType, component, subscriberID, name)
	sm.subscribers[subscriberID] = subscriber

	log.WithFields(log.Fields{
		"subscriberID": subscriberID,
		"name":         name,
		"type":         subscriber_manager_pb.SubscriberType_name[int32(subscriberType)],
		"component":    component,
	}).Info("Registered subscriber")

	// call synchronizer api to register subscriber
	for synchronizerID, _ := range sm.controller.synchronizerManager.GetSynchronizers() {
		err := sm.register(synchronizerID, subscriberID)
		if err != nil {
			return subscriber, errors.New("Failed to register subscriber on eventstore: " + synchronizerID)
		}
	}

	return subscriber, nil
}

func (sm *SubscriberManager) Unregister(subscriberID string) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	/*
		subscriber, ok := controller.subscriberClients[subscriberID]
		if ok {
			return nil
		}

		//TODO call synchronizer api to stop send event
	*/

	// Take off subscriber from registry
	delete(sm.subscribers, subscriberID)

	return nil
}

func (sm *SubscriberManager) GetSubscriber(subscriberID string) *SubscriberClient {

	subscriber, ok := sm.subscribers[subscriberID]
	if !ok {
		return nil
	}

	return subscriber
}

func (sm *SubscriberManager) GetSubscribers() ([]*SubscriberClient, error) {

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	subscribers := make([]*SubscriberClient, 0, len(sm.subscribers))
	for _, subscriber := range sm.subscribers {
		subscribers = append(subscribers, subscriber)
	}

	return subscribers, nil
}
