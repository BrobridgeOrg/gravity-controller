package controller

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
)

func (sm *SubscriberManager) initialize_rpc() error {

	log.Info("Initializing RPC Handlers for SubscriberManager")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": middleware.Authentication{
			Enabled: sm.requiredAuth,
			Keyring: sm.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	sm.rpcEngine = broc.NewBroc(sm.controller.gravityClient.GetConnection())
	sm.rpcEngine.Use(m.PacketHandler)
	sm.rpcEngine.SetPrefix(fmt.Sprintf("%s.", sm.controller.domain))

	// Register methods
	sm.rpcEngine.Register("subscriber_manager.registerSubscriber", sm.rpc_registerSubscriber)
	sm.rpcEngine.Register("subscriber_manager.unregisterSubscriber", m.RequiredAuth(), sm.rpc_unregisterSubscriber)
	sm.rpcEngine.Register("subscriber_manager.healthCheck", m.RequiredAuth(), sm.rpc_healthCheck)
	sm.rpcEngine.Register("subscriber_manager.getSubscribers",
		m.RequiredAuth("SYSTEM", "ADMIN", "SUBSCRIBER_MANAGER_ADMIN"),
		sm.rpc_getSubscribers,
	)
	sm.rpcEngine.Register("subscriber_manager.subscribeToCollections", m.RequiredAuth(), sm.rpc_subscribeToCollections)

	return sm.rpcEngine.Apply()
}

func (sm *SubscriberManager) rpc_registerSubscriber(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := subscriber_manager_pb.RegisterSubscriberReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req subscriber_manager_pb.RegisterSubscriberRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Register subscriber on all synchronizers
	props := map[string]interface{}{
		"token": req.Token,
	}
	err = sm.Register(
		req.Type,
		req.Component,
		req.AppID,
		req.Token,
		req.SubscriberID,
		req.Name,
		props,
	)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (sm *SubscriberManager) rpc_unregisterSubscriber(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := subscriber_manager_pb.UnregisterSubscriberReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req subscriber_manager_pb.UnregisterSubscriberRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Unregister subscriber on all synchronizer nodes
	err = sm.Unregister(req.SubscriberID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (sm *SubscriberManager) rpc_healthCheck(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := subscriber_manager_pb.HealthCheckReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req subscriber_manager_pb.HealthCheckRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
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

	return
}

func (sm *SubscriberManager) rpc_getSubscribers(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := subscriber_manager_pb.GetSubscribersReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req subscriber_manager_pb.GetSubscribersRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
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

		appID := ""
		v, ok := subscriber.properties["auth.appID"]
		if ok {
			appID = v.(string)
		}

		subscribers = append(subscribers, &subscriber_manager_pb.Subscriber{
			SubscriberID: subscriber.id,
			Name:         subscriber.name,
			Type:         subscriber.subscriberType,
			Component:    subscriber.component,
			LastCheck:    lastCheck,
			AppID:        appID,
		})
	}

	reply.Subscribers = subscribers

	return
}

func (sm *SubscriberManager) rpc_subscribeToCollections(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := subscriber_manager_pb.SubscribeToCollectionsReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req subscriber_manager_pb.SubscribeToCollectionsRequest
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Subscribe to collections
	subscriber := sm.GetSubscriber(req.SubscriberID)
	if subscriber == nil {
		log.Errorf("Not found subscriber: %s", req.SubscriberID)
		reply.Success = false
		reply.Reason = "NotFoundSubscriber"
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

	return
}
