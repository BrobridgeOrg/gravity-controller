package controller

import (
	"fmt"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	synchronizer_manager_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer_manager"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (sm *SynchronizerManager) initializeRPC() error {

	log.Info("Initializing RPC Handlers for SynchronizerManager")

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
	sm.rpcEngine.SetPrefix(fmt.Sprintf("%s.synchronizer_manager.", sm.controller.domain))

	// Register methods
	sm.rpcEngine.Register("register", m.RequiredAuth("SYSTEM"), sm.rpc_register)
	sm.rpcEngine.Register("unregister", m.RequiredAuth("SYSTEM"), sm.rpc_unregister)
	sm.rpcEngine.Register("getPipelines", m.RequiredAuth("SYSTEM"), sm.rpc_getPipelines)

	return sm.rpcEngine.Apply()
}

func (sm *SynchronizerManager) rpc_register(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := synchronizer_manager_pb.RegisterSynchronizerReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req synchronizer_manager_pb.RegisterSynchronizerRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Register
	err = sm.Register(req.SynchronizerID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	log.WithFields(log.Fields{
		"id": req.SynchronizerID,
	}).Info("Registered synchronizer")

	return
}

func (sm *SynchronizerManager) rpc_unregister(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := synchronizer_manager_pb.UnregisterSynchronizerReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req synchronizer_manager_pb.UnregisterSynchronizerRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Unregister
	err = sm.Unregister(req.SynchronizerID)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = err.Error()
		return
	}

	return
}

func (sm *SynchronizerManager) rpc_getPipelines(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := synchronizer_manager_pb.GetPipelinesReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req synchronizer_manager_pb.GetPipelinesRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Getting specific synchronizer
	synchronizer := sm.GetSynchronizer(req.SynchronizerID)
	if synchronizer == nil {
		log.WithFields(log.Fields{
			"id": req.SynchronizerID,
		}).Error("Not found synchronizer")
		reply.Success = false
		reply.Reason = "NotFound"
		return
	}

	reply.Pipelines = synchronizer.pipelines

	return
}
