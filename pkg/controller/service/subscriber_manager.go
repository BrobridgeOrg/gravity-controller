package controller

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/BrobridgeOrg/broc"
	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type SubscriberManager struct {
	controller     *Controller
	allowAnonymous bool
	rpcEngine      *broc.Broc
	subscribers    map[string]*Subscriber
	mutex          sync.RWMutex
}

func NewSubscriberManager(controller *Controller) *SubscriberManager {
	return &SubscriberManager{
		controller:     controller,
		allowAnonymous: false,
		subscribers:    make(map[string]*Subscriber),
	}
}

func (sm *SubscriberManager) Initialize() error {

	// Load configurations
	viper.SetDefault("subscriber_manager.allowAnonymous", true)
	sm.allowAnonymous = viper.GetBool("subscriber_manager.allowAnonymous")
	if sm.allowAnonymous {
		key := sm.controller.keyring.Get("anonymous")
		if key == nil {
			key = sm.controller.keyring.Put("anonymous", "")
		}

		key.Permission().AddPermissions([]string{"SUBSCRIBER"})
	}

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
			data["properties"].(map[string]interface{}),
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

	err = sm.initializeRPC()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SubscriberManager) addSubscriber(subscriberType subscriber_manager_pb.SubscriberType, component string, subscriberID string, name string, properties map[string]interface{}) (*Subscriber, error) {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.subscribers[subscriberID]
	if ok {
		return nil, errors.New("Exists")
	}

	// Create a new subscriber
	subscriber := NewSubscriber(sm.controller, subscriberType, component, subscriberID, name, properties)
	sm.subscribers[subscriberID] = subscriber

	return subscriber, nil
}

func (sm *SubscriberManager) register(eventstoreID string, subscriberID string, appID string, accessKey string) error {

	request := synchronizer_pb.RegisterSubscriberRequest{
		SubscriberID: subscriberID,
		Name:         subscriberID,
		AppID:        appID,
		AccessKey:    accessKey,
	}

	msg, _ := proto.Marshal(&request)

	respData, err := sm.controller.synchronizerManager.Request(eventstoreID, "registerSubscriber", msg)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.RegisterSubscriberReply
	err = proto.Unmarshal(respData, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		log.Error(reply.Reason)
		return err
	}

	return nil
}

func (sm *SubscriberManager) Register(subscriberType subscriber_manager_pb.SubscriberType, component string, appID string, token []byte, subscriberID string, name string, properties map[string]interface{}) error {

	key := sm.controller.auth.Authenticate(appID, token, sm.allowAnonymous)
	if key == nil {
		return errors.New("Forbidden")
	}

	properties["auth.appID"] = appID
	properties["auth.appKey"] = string(key.Encryption().GetKey())

	// Update keyring to syncronizer
	sm.controller.synchronizerManager.UpdateKeyring(key)

	subscriber, err := sm.addSubscriber(subscriberType, component, subscriberID, name, properties)
	if err != nil {
		if err.Error() == "Exists" {
			log.WithFields(log.Fields{
				"subscriber": subscriberID,
			}).Warn("Subscriber exists already")
			return nil
		}

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
		err := sm.register(synchronizerID, subscriberID, appID, string(key.Encryption().GetKey()))
		if err != nil {
			// Ignore
			log.Errorf("Failed to register subscriber on eventstore: %s", synchronizerID)
			continue
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

	// Release
	subscriber, ok := sm.subscribers[subscriberID]
	if !ok {
		return nil
	}

	if sm.controller.auth.enabledAuthService {
		appID := subscriber.properties["auth.appID"].(string)
		sm.controller.keyring.Unref(appID)
	}

	subscriber.release()

	// Remove subscriber from registry
	delete(sm.subscribers, subscriberID)

	return nil
}

func (sm *SubscriberManager) HealthCheck(subscriberID string) error {

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	subscriber, ok := sm.subscribers[subscriberID]
	if !ok {
		return nil
	}

	return subscriber.healthCheck()
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
