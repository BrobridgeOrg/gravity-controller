package controller

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	collection_manager_pb "github.com/BrobridgeOrg/gravity-api/service/collection_manager"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
	"github.com/BrobridgeOrg/gravity-sdk/collection_manager/types"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (cm *CollectionManager) initializeRPC() error {

	log.Info("Initializing RPC Handlers for CollectionManager")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: cm.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	cm.rpcEngine = broc.NewBroc(cm.controller.gravityClient.GetConnection())
	cm.rpcEngine.Use(m.PacketHandler)
	cm.rpcEngine.SetPrefix(fmt.Sprintf("%s.collection_manager.", cm.controller.domain))

	// Register methods
	cm.rpcEngine.Register("register", m.RequiredAuth("SYSTEM"), cm.rpc_register)
	cm.rpcEngine.Register("unregister", m.RequiredAuth("SYSTEM"), cm.rpc_unregister)
	cm.rpcEngine.Register("getCollection",
		m.RequiredAuth("SYSTEM", "SUBSCRIBER"),
		cm.rpc_getCollection,
	)
	cm.rpcEngine.Register("getCollections",
		m.RequiredAuth("SYSTEM", "SUBSCRIBER"),
		cm.rpc_getCollections,
	)

	return cm.rpcEngine.Apply()
}

func (cm *CollectionManager) rpc_register(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := collection_manager_pb.RegisterReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req collection_manager_pb.RegisterRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Prepare collection information
	collection := types.UnmarshalProto(req.Collection)

	// Register collection
	err = cm.Register(req.Collection.CollectionID, collection)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (cm *CollectionManager) rpc_unregister(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := collection_manager_pb.UnregisterReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req collection_manager_pb.UnregisterRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Unregister collection on all synchronizer nodes
	err = cm.Unregister(req.CollectionID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (cm *CollectionManager) rpc_getCollection(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := collection_manager_pb.GetCollectionReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req collection_manager_pb.GetCollectionRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Gettting collection list
	collection, err := cm.GetCollection(req.CollectionID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	reply.Collection = types.MarshalProto(collection)

	return
}

func (cm *CollectionManager) rpc_getCollections(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := collection_manager_pb.GetCollectionsReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req collection_manager_pb.GetCollectionsRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Gettting collection list
	results, err := cm.GetCollections()
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	// Preparing results
	collections := make([]*collection_manager_pb.Collection, len(results))
	for i, collection := range results {
		collections[i] = types.MarshalProto(collection)
	}

	reply.Collections = collections

	return
}
