package controller

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	pb "github.com/BrobridgeOrg/gravity-api/service/pipeline_manager"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller/service/middleware"
)

func (pm *PipelineManager) initializeRPC() error {

	// Initializing authentication middleware
	m := middleware.NewMiddleware(map[string]interface{}{
		"Authentication": &middleware.Authentication{
			Enabled: true,
			Keyring: pm.controller.keyring,
		},
	})

	// Initializing RPC engine to handle requests
	pm.rpcEngine = broc.NewBroc(pm.controller.gravityClient.GetConnection())
	pm.rpcEngine.Use(m.PacketHandler)
	pm.rpcEngine.SetPrefix(fmt.Sprintf("%s.pipeline_manager.", pm.controller.domain))

	// Register methods
	pm.rpcEngine.Register("getCount", m.RequiredAuth("SYSTEM", "SUBSCRIBER"), pm.rpc_getCount)

	return pm.rpcEngine.Apply()
}

func (pm *PipelineManager) rpc_getCount(ctx *broc.Context) (returnedValue interface{}, err error) {

	// Reply
	reply := pb.GetPipelineCountReply{
		Success: true,
	}
	defer func() {
		data, e := proto.Marshal(&reply)
		returnedValue = data
		err = e
	}()

	// Parsing request data
	var req pb.GetPipelineCountRequest
	payload := ctx.Get("payload").(*packet_pb.Payload)
	err = proto.Unmarshal(payload.Data, &req)
	if err != nil {
		log.Error(err)

		reply.Success = false
		reply.Reason = "UnknownParameter"
		return
	}

	// Start transmitter on all synchronizer nodes
	reply.Count = pm.controller.GetPipelineCount()

	return
}
