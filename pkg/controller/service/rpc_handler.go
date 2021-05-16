package controller

import (
	pb "github.com/BrobridgeOrg/gravity-api/service/controller"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (controller *Controller) InitRPCHandlers() error {

	err := controller.initRPC_SubscriberRegister()
	if err != nil {
		return err
	}

	err = controller.initRPC_SubscriberUnregister()
	if err != nil {
		return err
	}

	err = controller.initRPC_GetPipelineCount()
	if err != nil {
		return err
	}

	err = controller.initRPC_SubscribeToCollections()
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_SubscriberRegister() error {

	connection := controller.eventBus.bus.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.core.registerSubscriber",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.core.registerSubscriber", func(m *nats.Msg) {

		// Parsing request data
		var req pb.RegisterSubscriberRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply := pb.RegisterSubscriberReply{
				Success: false,
				Reason:  "UnknownParameter",
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Register transmitter on all synchronizer nodes
		subscriber, err := controller.RegisterSubscriber(req.SubscriberID)
		if err != nil {
			log.Error(err)

			reply := pb.RegisterSubscriberReply{
				Success: false,
				Reason:  err.Error(),
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Reply
		reply := pb.RegisterSubscriberReply{
			Success: true,
			Channel: subscriber.GetChannel(),
		}

		data, _ := proto.Marshal(&reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_SubscriberUnregister() error {

	connection := controller.eventBus.bus.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.core.unregisterSubscriber",
	}).Info("Unsubscribing to channel")

	_, err := connection.Subscribe("gravity.core.unregisterSubscriber", func(m *nats.Msg) {

		// Parsing request data
		var req pb.UnregisterSubscriberRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply := pb.UnregisterSubscriberReply{
				Success: false,
				Reason:  "UnknownParameter",
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Register transmitter on all synchronizer nodes
		err = controller.UnregisterSubscriber(req.SubscriberID)
		if err != nil {
			log.Error(err)

			reply := pb.UnregisterSubscriberReply{
				Success: false,
				Reason:  err.Error(),
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Reply
		reply := pb.UnregisterSubscriberReply{
			Success: true,
		}

		data, _ := proto.Marshal(&reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_GetPipelineCount() error {

	connection := controller.eventBus.bus.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.core.getPipelineCount",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.core.getPipelineCount", func(m *nats.Msg) {

		log.Info("gravity.core.getPipelineCount")

		// Parsing request data
		var req pb.GetPipelineCountRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply := pb.GetPipelineCountReply{
				Success: false,
				Reason:  "UnknownParameter",
			}

			data, _ := proto.Marshal(&reply)
			m.Respond(data)

			return
		}

		// Start transmitter on all synchronizer nodes
		count := controller.GetPipelineCount()

		// Reply
		reply := pb.GetPipelineCountReply{
			Success: true,
			Count:   count,
		}

		data, _ := proto.Marshal(&reply)
		m.Respond(data)
	})
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_SubscribeToCollections() error {

	connection := controller.eventBus.bus.GetConnection()

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
