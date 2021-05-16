package controller

import (
	"errors"
	"fmt"
	"time"

	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	def "github.com/BrobridgeOrg/gravity-controller/pkg/controller"
	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

func (controller *Controller) registerSubscriber(eventstoreID string, subscriberID string) error {

	channel := fmt.Sprintf("gravity.eventstore.%s.registerSubscriber", eventstoreID)

	request := synchronizer_pb.RegisterSubscriberRequest{
		SubscriberID: subscriberID,
		Name:         subscriberID,
	}

	msg, _ := proto.Marshal(&request)

	conn := controller.eventBus.bus.GetConnection()
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

func (controller *Controller) RegisterSubscriber(subscriberID string) (def.SubscriberClient, error) {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	_, ok := controller.subscriberClients[subscriberID]
	if ok {
		return nil, errors.New("Subscriber ID exists already")
	}

	// Create a new subscriber
	controller.channelCounter++
	channel := controller.channelCounter
	subscriber := NewSubscriberClient(controller, subscriberID, channel)
	controller.subscriberClients[subscriberID] = subscriber

	log.WithFields(log.Fields{
		"subscriberID": subscriberID,
	}).Info("Registered subscriber")

	// call synchronizer api to register subscriber
	for synchronizerID, _ := range controller.clients {
		err := controller.registerSubscriber(synchronizerID, subscriberID)
		if err != nil {
			return subscriber, errors.New("Failed to register subscriber on eventstore: " + synchronizerID)
		}
	}

	return subscriber, nil
}

func (controller *Controller) UnregisterSubscriber(subscriberID string) error {

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	/*
		subscriber, ok := controller.subscriberClients[subscriberID]
		if ok {
			return nil
		}

		//TODO call synchronizer api to stop send event
	*/

	// Take off subscriber from registry
	delete(controller.subscriberClients, subscriberID)

	return nil
}

func (controller *Controller) SubscribeToCollections(subscriberID string, collections []string) ([]string, error) {

	subscriber, ok := controller.subscriberClients[subscriberID]
	if !ok {
		return nil, errors.New("No such subscriber")
	}

	return subscriber.SubscribeToCollections(collections)
}
