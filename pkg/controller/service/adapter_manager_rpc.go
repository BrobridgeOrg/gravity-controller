package controller

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	pb "github.com/BrobridgeOrg/gravity-api/service/adapter_manager"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
)

func (am *AdapterManager) initialize_rpc() error {

	log.Info("Initializing RPC Handlers for AdapterManager")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: am.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	am.rpcEngine = broc.NewBroc(am.controller.gravityClient.GetConnection())
	am.rpcEngine.Use(m.PacketHandler)
	am.rpcEngine.SetPrefix(fmt.Sprintf("%s.adapter_manager.", am.controller.domain))

	// Register methods
	am.rpcEngine.Register("register", m.RequiredAuth("ADAPTER"), am.rpc_register)
	am.rpcEngine.Register("unregister", m.RequiredAuth("ADAPTER"), am.rpc_unregister)
	am.rpcEngine.Register("getAdapters", m.RequiredAuth("SYSTEM", "ADAPTER_MANAGER"), am.rpc_getAdapters)

	return am.rpcEngine.Apply()
}

func (am *AdapterManager) rpc_register(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := pb.RegisterAdapterReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req pb.RegisterAdapterRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Authenticate
	key := am.controller.auth.Authenticate(req.AppID, req.Token, am.allowAnonymous)
	if key == nil {
		reply.Success = false
		reply.Reason = "Forbidden"
		return
	}

	// Register
	err = am.Register(req.Component, req.AdapterID, req.Name, key)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (am *AdapterManager) rpc_unregister(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := pb.UnregisterAdapterReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req pb.UnregisterAdapterRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	err = am.Unregister(req.AdapterID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (am *AdapterManager) rpc_getAdapters(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := pb.GetAdaptersReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req pb.GetAdaptersRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Gettting adapter list
	results, err := am.GetAdapters()
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	// Preparing results
	adapters := make([]*pb.Adapter, 0, len(results))
	for _, adapter := range results {

		adapters = append(adapters, &pb.Adapter{
			AdapterID: adapter.id,
			Name:      adapter.name,
			Component: adapter.component,
		})
	}

	reply.Adapters = adapters

	return
}
