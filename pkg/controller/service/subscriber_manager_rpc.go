package controller

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	subscriber_manager_pb "github.com/BrobridgeOrg/gravity-api/service/subscriber_manager"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
)

func (sm *SubscriberManager) initializeRPC() error {

	log.Info("Initializing RPC Handlers for SubscriberManager")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: sm.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	sm.rpcEngine = broc.NewBroc(sm.controller.gravityClient.GetConnection())
	sm.rpcEngine.Use(m.PacketHandler)
	sm.rpcEngine.SetPrefix(fmt.Sprintf("%s.subscriber_manager.", sm.controller.domain))

	// Register methods
	sm.rpcEngine.Register("registerSubscriber", m.RequiredAuth(), sm.rpc_registerSubscriber)
	sm.rpcEngine.Register("unregisterSubscriber", m.RequiredAuth("SUBSCRIBER"), sm.rpc_unregisterSubscriber)
	sm.rpcEngine.Register("updateSubscriberProps",
		m.RequiredAuth("SYSTEM", "SUBSCRIBER_MANAGER", "SUBSCRIBER"),
		sm.rpc_updateSubscriberProps,
	)
	sm.rpcEngine.Register("healthCheck", m.RequiredAuth("SUBSCRIBER"), sm.rpc_healthCheck)
	sm.rpcEngine.Register("getSubscribers",
		m.RequiredAuth("SYSTEM", "SUBSCRIBER_MANAGER"),
		sm.rpc_getSubscribers,
	)
	sm.rpcEngine.Register("subscribeToCollections", m.RequiredAuth("SUBSCRIBER"), sm.rpc_subscribeToCollections)

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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
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

func (sm *SubscriberManager) rpc_updateSubscriberProps(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := subscriber_manager_pb.UpdateSubscriberPropsReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req subscriber_manager_pb.UpdateSubscriberPropsRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Register subscriber on all synchronizers
	props := make(map[string]interface{})

	// Update pipelines
	if len(req.Pipelines) > 0 {
		pipelines := make([]uint64, len(req.Pipelines))

		for _, pid := range req.Pipelines {
			pipelines = append(pipelines, pid)
		}

		props["pipelines"] = pipelines
	}

	err = sm.UpdateSubscriberProps(
		req.SubscriberID,
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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
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
	subscribers := make([]*subscriber_manager_pb.Subscriber, len(results))
	for i, subscriber := range results {

		lastCheck, _ := ptypes.TimestampProto(subscriber.lastCheck)

		appID := ""
		v, ok := subscriber.properties["auth.appID"]
		if ok {
			appID = v.(string)
		}

		s := &subscriber_manager_pb.Subscriber{
			SubscriberID: subscriber.id,
			Name:         subscriber.name,
			Type:         subscriber.subscriberType,
			Component:    subscriber.component,
			Collections:  subscriber.GetCollections(),
			LastCheck:    lastCheck,
			AppID:        appID,
		}

		// Collections
		if subscriber.properties["collections"] != nil {
			collections := make([]string, 0)
			for _, c := range subscriber.properties["collections"].([]string) {
				collections = append(collections, c)
			}

			s.Collections = collections
		}

		// Pipelines
		if subscriber.properties["pipelines"] != nil {
			pipelines := make([]uint64, 0)
			for _, pid := range subscriber.properties["pipelines"].([]uint64) {
				pipelines = append(pipelines, pid)
			}

			s.Pipelines = pipelines
		}

		subscribers[i] = s
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
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	if len(req.Collections) == 0 {
		reply.Success = false
		reply.Reason = "InvalidParameters"
		return
	}

	// Check collection permission
	key := ctx.Get("key").(*keyring.KeyInfo)
	targetCollections := make([]string, 0)
	for _, collection := range req.Collections {

		// Ignore collection that no permission to access
		if !key.Collection().Check(collection) {
			continue
		}

		targetCollections = append(targetCollections, collection)
	}

	if len(targetCollections) == 0 {
		reply.Success = false
		reply.Reason = "NoPermission"
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
