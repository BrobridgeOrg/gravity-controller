package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type SubscriberManager struct {
	controller  *Controller
	subscribers map[string]*Subscriber
	mutex       sync.RWMutex
}

func NewSubscriberManager(controller *Controller) *SubscriberManager {
	return &SubscriberManager{
		controller:  controller,
		subscribers: make(map[string]*Subscriber),
	}
}

func (sm *SubscriberManager) Initialize() error {

	// Restore states from store
	store, err := sm.controller.store.GetEngine().GetStore("gravity_subscriber_manager")
	if err != nil {
		return nil
	}

	err = store.RegisterColumns([]string{"subscribers"})
	if err != nil {
		return nil
	}

	log.Info("Trying to restoring subscribers...")

	store.List("subscribers", []byte(""), func(key []byte, value []byte) bool {

		var data map[string]interface{}
		json.Unmarshal(value, &data)

		subscriber, err := sm.addSubscriber(
			subscriber_manager_pb.SubscriberType(data["type"].(float64)),
			data["component"].(string),
			data["id"].(string),
			data["name"].(string),
		)
		if err != nil {
			log.Error(err)
			return false
		}

		log.WithFields(log.Fields{
			"id":        subscriber.id,
			"name":      subscriber.name,
			"component": subscriber.component,
			"type":      subscriber_manager_pb.SubscriberType_name[int32(subscriber.subscriberType)],
		}).Info("Restored subscriber")

		if data["collections"] != nil {
			cols := data["collections"].([]interface{})

			collections := make([]string, 0, len(cols))
			for _, col := range cols {
				if col == nil {
					continue
				}

				collections = append(collections, col.(string))
				log.Info("  collection: " + col.(string))
			}

			subscriber.addCollections(collections)
		}

		return true
	})

	err = sm.initialize_rpc()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SubscriberManager) addSubscriber(subscriberType subscriber_manager_pb.SubscriberType, component string, subscriberID string, name string) (*Subscriber, error) {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.subscribers[subscriberID]
	if ok {
		return nil, errors.New("Subscriber ID exists already")
	}

	// Create a new subscriber
	subscriber := NewSubscriber(sm.controller, subscriberType, component, subscriberID, name)
	sm.subscribers[subscriberID] = subscriber

	return subscriber, nil
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

func (sm *SubscriberManager) Register(subscriberType subscriber_manager_pb.SubscriberType, component string, subscriberID string, name string) error {

	subscriber, err := sm.addSubscriber(subscriberType, component, subscriberID, name)
	if err != nil {
		return err
	}

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
			return errors.New("Failed to register subscriber on eventstore: " + synchronizerID)
		}
	}

	// Save state
	err = subscriber.save()
	if err != nil {
		log.Error(err)
	}

	return nil
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

func (sm *SubscriberManager) GetSubscriber(subscriberID string) *Subscriber {

	subscriber, ok := sm.subscribers[subscriberID]
	if !ok {
		return nil
	}

	return subscriber
}

func (sm *SubscriberManager) GetSubscribers() ([]*Subscriber, error) {

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	subscribers := make([]*Subscriber, 0, len(sm.subscribers))
	for _, subscriber := range sm.subscribers {
		subscribers = append(subscribers, subscriber)
	}

	return subscribers, nil
}
