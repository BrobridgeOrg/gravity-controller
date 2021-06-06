package controller

import (
	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (sm *SubscriberManager) initialize_rpc() error {

	err := sm.initialize_rpc_register()
	if err != nil {
		return err
	}

	err = sm.initialize_rpc_unregister()
	if err != nil {
		return err
	}

	err = sm.initialize_rpc_health_check()
	if err != nil {
		return err
	}

	err = sm.initialize_rpc_get_subscribers()
	if err != nil {
		return err
	}

	err = sm.initialize_rpc_subscribe_to_collections()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SubscriberManager) initialize_rpc_register() error {

	connection := sm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.subscriber_manager.registerSubscriber",
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
		err = sm.Register(req.Type, req.Component, req.SubscriberID, req.Name)
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

func (sm *SubscriberManager) initialize_rpc_unregister() error {

	connection := sm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.subscriber_manager.unregisterSubscriber",
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
		err = sm.Unregister(req.SubscriberID)
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

func (sm *SubscriberManager) initialize_rpc_health_check() error {

	connection := sm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.subscriber_manager.healthCheck",
	}).Info("Unsubscribing to channel")

	_, err := connection.Subscribe("gravity.subscriber_manager.healthCheck", func(m *nats.Msg) {

		// Reply
		reply := subscriber_manager_pb.HealthCheckReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req subscriber_manager_pb.HealthCheckRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		err = sm.HealthCheck(req.SubscriberID)
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

func (sm *SubscriberManager) initialize_rpc_get_subscribers() error {

	connection := sm.controller.gravityClient.GetConnection()

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
		results, err := sm.GetSubscribers()
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}

		// Preparing results
		subscribers := make([]*subscriber_manager_pb.Subscriber, 0, len(results))
		for _, subscriber := range results {

			lastCheck, _ := ptypes.TimestampProto(subscriber.lastCheck)

			subscribers = append(subscribers, &subscriber_manager_pb.Subscriber{
				SubscriberID: subscriber.id,
				Name:         subscriber.name,
				Type:         subscriber.subscriberType,
				Component:    subscriber.component,
				LastCheck:    lastCheck,
			})
		}

		reply.Subscribers = subscribers
	})
	if err != nil {
		return err
	}

	return nil
}

func (sm *SubscriberManager) initialize_rpc_subscribe_to_collections() error {

	connection := sm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.subscriber_manager.registerSubscriber",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.subscriber_manager.subscribeToCollections", func(m *nats.Msg) {

		// Reply
		reply := subscriber_manager_pb.SubscribeToCollectionsReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req subscriber_manager_pb.SubscribeToCollectionsRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		// Subscribe to collections
		subscriber := sm.GetSubscriber(req.SubscriberID)
		if subscriber == nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}

		collections, err := subscriber.SubscribeToCollections(req.Collections)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}

		reply.Collections = collections
	})
	if err != nil {
		return err
	}

	return nil
}
