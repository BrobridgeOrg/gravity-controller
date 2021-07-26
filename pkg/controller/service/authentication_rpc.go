package controller

import (
	"encoding/json"
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

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": middleware.Authentication{
			Enabled: true,
			Keyring: auth.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	auth.rpcEngine = broc.NewBroc(auth.controller.gravityClient.GetConnection())
	auth.rpcEngine.Use(m.PacketHandler)
	auth.rpcEngine.Use(m.RequiredAuth(
		"SYSTEM",
		"ADMIN",
		"AUTHENTICATION_MANAGER_ADMIN",
	))

	auth.rpcEngine.SetPrefix(fmt.Sprintf("%s.authentication_manager", auth.controller.domain))

	// Register methods
	auth.rpcEngine.Register("createEntity", auth.rpc_createEntity)
	auth.rpcEngine.Register("deleteEntity", auth.rpc_deleteEntity)

	return nil
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Prepare new entity
	entity := &authenticator.Entity{
		AppID:      req.Entity.AppID,
		AppName:    req.Entity.AppName,
		AccessKey:  req.Entity.Key,
		Properties: make(map[string]interface{}),
	}

	// Property is not empty
	if len(req.Entity.Properties) > 0 {
		var props map[string]interface{}
		err = json.Unmarshal(req.Entity.Properties, &props)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}

		// Append properties
		for k, v := range props {
			entity.Properties[k] = v
		}
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
	packet := ctx.Get("request").(*packet_pb.Packet)
	err = proto.Unmarshal(packet.Payload, &req)
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
