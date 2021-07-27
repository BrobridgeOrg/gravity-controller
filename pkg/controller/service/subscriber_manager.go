package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
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
		keyInfo := sm.controller.keyring.Put("anonymous", "")
		keyInfo.Permission().AddPermissions([]string{"SUBSCRIBER"})
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

func (sm *SubscriberManager) request(eventstoreID string, method string, data []byte, encrypted bool) ([]byte, error) {

	conn := sm.controller.gravityClient.GetConnection()

	// Preparing packet
	packet := packet_pb.Packet{
		AppID:   "gravity",
		Payload: data,
	}

	// find the key for gravity
	keyInfo := sm.controller.keyring.Get("gravity")
	if keyInfo == nil {
		return []byte(""), errors.New("No access key for gravity")
	}

	// Encrypt
	if encrypted {
		payload, err := keyInfo.Encryption().Encrypt(data)
		if err != nil {
			return []byte(""), err
		}

		packet.Payload = payload
	}

	msg, _ := proto.Marshal(&packet)

	// Send request
	channel := fmt.Sprintf("%s.eventstore.%s.%s", sm.controller.domain, eventstoreID, method)
	resp, err := conn.Request(channel, msg, time.Second*10)
	if err != nil {
		return []byte(""), err
	}

	// Decrypt
	if encrypted {
		data, err = keyInfo.Encryption().Decrypt(resp.Data)
		if err != nil {
			return []byte(""), err
		}

		return data, nil
	}

	return resp.Data, nil
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

	respData, err := sm.request(eventstoreID, "registerSubscriber", msg, true)
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

	accessKey := ""

	if sm.controller.auth.enabledAuthService {
		entity, err := sm.controller.auth.authenticator.Authenticate(appID, token)
		if err != nil {
			return err
		}

		properties["auth.appID"] = entity.AppID
		properties["auth.appKey"] = entity.AccessKey
		accessKey = entity.AccessKey

		// Add to keyring
		sm.controller.keyring.Put(entity.AppID, entity.AccessKey)
	} else if sm.allowAnonymous {

		if appID != "anonymous" {
			return errors.New("Forbidden")
		}
	}

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
		err := sm.register(synchronizerID, subscriberID, appID, accessKey)
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
