package controller

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	auth_pb "github.com/BrobridgeOrg/gravity-api/service/auth"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
	authenticator "github.com/BrobridgeOrg/gravity-sdk/authenticator"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (auth *Authentication) InitializeRPC() error {

	log.Info("Initializing RPC Handlers for AuthenticationManager")

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: auth.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	auth.rpcEngine = broc.NewBroc(auth.controller.gravityClient.GetConnection())
	auth.rpcEngine.Use(auth.rpc_middleware)
	auth.rpcEngine.Use(m.PacketHandler)
	auth.rpcEngine.Use(m.RequiredAuth(
		"SYSTEM",
		"ADMIN",
		"AUTHENTICATION_MANAGER_ADMIN",
	))

	auth.rpcEngine.SetPrefix(fmt.Sprintf("%s.authentication_manager.", auth.controller.domain))

	// Register methods
	auth.rpcEngine.Register("createEntity", auth.rpc_createEntity)
	auth.rpcEngine.Register("updateEntity", auth.rpc_updateEntity)
	auth.rpcEngine.Register("deleteEntity", auth.rpc_deleteEntity)
	auth.rpcEngine.Register("getEntity", auth.rpc_getEntity)
	auth.rpcEngine.Register("updateEntityKey", auth.rpc_updateEntityKey)
	auth.rpcEngine.Register("getEntities", auth.rpc_getEntities)

	return auth.rpcEngine.Apply()
}

func (auth *Authentication) rpc_middleware(ctx *broc.Context) (returnedValue interface{}, err error) {

	if !auth.enabledAuthService {
		return nil, nil
	}

	return ctx.Next()
}

func (auth *Authentication) rpc_createEntity(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := auth_pb.CreateEntityReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req auth_pb.CreateEntityRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Prepare new entity
	entity, err := authenticator.ParseEntityProto(req.Entity)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	err = auth.authenticator.CreateEntity(entity)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (auth *Authentication) rpc_updateEntity(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := auth_pb.UpdateEntityReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req auth_pb.UpdateEntityRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Prepare entity
	entity, err := authenticator.ParseEntityProto(req.Entity)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	err = auth.authenticator.UpdateEntity(entity)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (auth *Authentication) rpc_deleteEntity(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := auth_pb.DeleteEntityReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req auth_pb.DeleteEntityRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	err = auth.authenticator.DeleteEntity(req.AppID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (auth *Authentication) rpc_getEntity(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := auth_pb.GetEntityReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req auth_pb.GetEntityRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	entity, err := auth.authenticator.GetEntity(req.AppID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	reply.Entity = authenticator.ConvertEntityToProto(entity)

	return
}

func (auth *Authentication) rpc_updateEntityKey(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := auth_pb.UpdateEntityKeyReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req auth_pb.UpdateEntityKeyRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	err = auth.authenticator.UpdateEntityKey(req.AppID, req.Key)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (auth *Authentication) rpc_getEntities(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := auth_pb.GetEntitiesReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req auth_pb.GetEntitiesRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	entities, total, err := auth.authenticator.GetEntities(req.StartID, req.Count)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	reply.Total = total

	reply.Entities = make([]*auth_pb.Entity, len(entities))
	for i, entity := range entities {
		reply.Entities[i] = authenticator.ConvertEntityToProto(entity)
	}

	return
}
