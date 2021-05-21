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

type Subscriber struct {
	controller     *Controller
	id             string
	name           string
	component      string
	subscriberType subscriber_manager_pb.SubscriberType
	collections    sync.Map
}

func NewSubscriber(controller *Controller, subscriberType subscriber_manager_pb.SubscriberType, component string, id string, name string) *Subscriber {
	return &Subscriber{
		controller:     controller,
		id:             id,
		name:           name,
		component:      component,
		subscriberType: subscriberType,
	}
}

func (sc *Subscriber) save() error {

	// Update store
	store, err := sc.controller.store.GetEngine().GetStore("gravity_subscriber_manager")
	if err != nil {
		return nil
	}

	collections := make([]string, 0)
	sc.collections.Range(func(key interface{}, value interface{}) bool {
		collections = append(collections, key.(string))
		return true
	})

	// Preparing JSON string
	data, err := json.Marshal(map[string]interface{}{
		"id":          sc.id,
		"name":        sc.name,
		"component":   sc.component,
		"type":        int32(sc.subscriberType),
		"collections": collections,
	})
	if err != nil {
		return err
	}

	err = store.Put("subscribers", []byte(sc.id), data)
	if err != nil {
		return err
	}

	return nil
}

func (sc *Subscriber) release() error {

	// Update store
	store, err := sc.controller.store.GetEngine().GetStore("gravity_subscriber_manager")
	if err != nil {
		return nil
	}

	return store.Delete("subscribers", []byte(sc.id))
}

func (sc *Subscriber) addCollections(collections []string) []string {

	results := make([]string, 0, len(collections))

	// Update collections table
	for _, col := range collections {
		if _, ok := sc.collections.Load(col); ok {
			results = append(results, col)
			continue
		}

		sc.collections.Store(col, true)
		results = append(results, col)
	}

	return results
}

func (sc *Subscriber) subscribeToCollections(eventstoreID string, collections []string) error {

	channel := fmt.Sprintf("gravity.eventstore.%s.subscribeToCollections", eventstoreID)

	request := synchronizer_pb.SubscribeToCollectionsRequest{
		SubscriberID: sc.id,
		Collections:  collections,
	}

	msg, _ := proto.Marshal(&request)

	conn := sc.controller.gravityClient.GetConnection()
	resp, err := conn.Request(channel, msg, time.Second*10)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.SubscribeToCollectionsReply
	err = proto.Unmarshal(resp.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (sc *Subscriber) SubscribeToCollections(collections []string) ([]string, error) {

	results := sc.addCollections(collections)

	// Call all synchronizers to subscribe
	for synchronizerID, _ := range sc.controller.synchronizerManager.GetSynchronizers() {
		err := sc.subscribeToCollections(synchronizerID, results)
		if err != nil {
			log.WithFields(log.Fields{
				"synchronizer": synchronizerID,
			}).Error(err)
		}
	}

	// Save state
	err := sc.save()
	if err != nil {
		log.Error(err)
	}

	return results, nil
}

func (sc *Subscriber) UnsubscribeFromCollections(collections []string) ([]string, error) {

	for _, col := range collections {
		sc.collections.Delete(col)
	}

	// TODO: call synchronizer

	return collections, nil
}
