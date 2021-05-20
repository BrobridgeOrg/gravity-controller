package controller

import (
	pb "github.com/BrobridgeOrg/gravity-api/service/controller"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (controller *Controller) InitRPCHandlers() error {

	err := controller.initRPC_SubscribeToCollections()
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_SubscribeToCollections() error {

	connection := controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.core.registerSubscriber",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.core.subscribeToCollections", func(m *nats.Msg) {

		log.Info("gravity.core.subscribeToCollections")

		// Parsing request data
		var req pb.SubscribeToCollectionsRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply := pb.SubscribeToCollectionsReply{
				Success: false,
				Reason:  "UnknownParameter",
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Subscribe to collections
		collections, err := controller.SubscribeToCollections(req.SubscriberID, req.Collections)
		if err != nil {
			log.Error(err)

			reply := pb.SubscribeToCollectionsReply{
				Success: false,
				Reason:  err.Error(),
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Reply
		reply := pb.SubscribeToCollectionsReply{
			Success:     true,
			Collections: collections,
		}

		data, _ := proto.Marshal(&reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	return nil
}
