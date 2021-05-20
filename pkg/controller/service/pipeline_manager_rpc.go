package controller

import (
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	pb "github.com/BrobridgeOrg/gravity-api/service/pipeline_manager"
)

func (pm *PipelineManager) initialize_rpc() error {

	err := pm.initialize_rpc_get_count()
	if err != nil {
		return err
	}

	return nil
}

func (pm *PipelineManager) initialize_rpc_get_count() error {

	connection := pm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.pipeline_manager.getCount",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.pipeline_manager.getCount", func(m *nats.Msg) {

		// Reply
		reply := pb.GetPipelineCountReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req pb.GetPipelineCountRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		// Start transmitter on all synchronizer nodes
		reply.Count = pm.controller.GetPipelineCount()
	})
	if err != nil {
		return err
	}

	return nil
}
