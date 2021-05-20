package controller

import (
	pb "github.com/BrobridgeOrg/gravity-api/service/controller"
	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
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

	err = controller.initRPC_SubscribeToCollections()
	if err != nil {
		return err
	}

	err = controller.initRPC_GetSubscribers()
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_SubscriberRegister() error {

	connection := controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.core.registerSubscriber",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.subscriber_manager.registerSubscriber", func(m *nats.Msg) {

		// Reply
		reply := subscriber_manager_pb.RegisterSubscriberReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req subscriber_manager_pb.RegisterSubscriberRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		// Register transmitter on all synchronizer nodes
		_, err = controller.RegisterSubscriber(req.Type, req.Component, req.SubscriberID, req.Name)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (controller *Controller) initRPC_SubscriberUnregister() error {

	connection := controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.core.unregisterSubscriber",
	}).Info("Unsubscribing to channel")

	_, err := connection.Subscribe("gravity.subscriber_manager.unregisterSubscriber", func(m *nats.Msg) {

		// Reply
		reply := subscriber_manager_pb.UnregisterSubscriberReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req subscriber_manager_pb.UnregisterSubscriberRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		// Register transmitter on all synchronizer nodes
		err = controller.UnregisterSubscriber(req.SubscriberID)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}
	})
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

func (controller *Controller) initRPC_GetSubscribers() error {

	connection := controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.subscriber_manager.getSubscribers",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.subscriber_manager.getSubscribers", func(m *nats.Msg) {

		log.Info("gravity.subscriber_manager.getSubscribers")

		// Reply
		reply := subscriber_manager_pb.GetSubscribersReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req subscriber_manager_pb.GetSubscribersRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		// Gettting subscriber list
		results, err := controller.GetSubscribers()
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}

		// Preparing results
		subscribers := make([]*subscriber_manager_pb.Subscriber, 0, len(results))
		for _, subscriber := range results {
			subscribers = append(subscribers, &subscriber_manager_pb.Subscriber{
				SubscriberID: subscriber.id,
				Name:         subscriber.name,
				Type:         subscriber.subscriberType,
				Component:    subscriber.component,
			})
		}

		reply.Subscribers = subscribers
	})
	if err != nil {
		return err
	}

	return nil
}
