package controller

import (
	"errors"
	"fmt"
	"time"

	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
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

	conn := controller.gravityClient.GetConnection()
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

func (controller *Controller) SubscribeToCollections(subscriberID string, collections []string) ([]string, error) {

	subscriber := controller.subscriberManager.GetSubscriber(subscriberID)
	if subscriber == nil {
		return nil, errors.New("No such subscriber")
	}

	return subscriber.SubscribeToCollections(collections)
}
