package controller

import (
	"errors"
	"fmt"
	"sync"
	"time"

	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

//"errors"
//"time"

//synchronizer "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
//"github.com/golang/protobuf/proto"

type SubscriberClient struct {
	controller     *Controller
	id             string
	name           string
	component      string
	subscriberType subscriber_manager_pb.SubscriberType
	collections    sync.Map
}

func NewSubscriberClient(controller *Controller, subscriberType subscriber_manager_pb.SubscriberType, component string, id string, name string) *SubscriberClient {
	return &SubscriberClient{
		controller:     controller,
		id:             id,
		name:           name,
		component:      component,
		subscriberType: subscriberType,
	}
}
func (sc *SubscriberClient) subscribeToCollections(eventstoreID string, collections []string) error {

	channel := fmt.Sprintf("gravity.eventstore.%s.subscribeToCollections", eventstoreID)

	request := synchronizer_pb.SubscribeToCollectionsRequest{
		SubscriberID: sc.id,
		Collections:  collections,
	}

	msg, _ := proto.Marshal(&request)

	conn := sc.controller.eventBus.bus.GetConnection()
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

func (sc *SubscriberClient) SubscribeToCollections(collections []string) ([]string, error) {

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

	// Call all synchronizers to subscribe
	for synchronizerID, _ := range sc.controller.clients {
		err := sc.subscribeToCollections(synchronizerID, results)
		if err != nil {
			log.WithFields(log.Fields{
				"synchronizer": synchronizerID,
			}).Error(err)
		}
	}

	return results, nil
}

func (sc *SubscriberClient) UnsubscribeFromCollections(collections []string) ([]string, error) {

	for _, col := range collections {
		sc.collections.Delete(col)
	}

	// TODO: call synchronizer

	return collections, nil
}
