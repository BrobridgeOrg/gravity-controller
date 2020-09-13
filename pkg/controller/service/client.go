package controller

import (
	"errors"
	"time"

	pb "github.com/BrobridgeOrg/gravity-api/service/controller"
	"github.com/golang/protobuf/proto"
)

type Client struct {
	controller *Controller
	id         string
	pipelines  []uint64
}

func NewClient(controller *Controller, id string) *Client {
	return &Client{
		controller: controller,
		id:         id,
	}
}

func (client *Client) AssignPipeline(pipelineID uint64) error {

	connection := client.controller.eventBus.bus.GetConnection()

	request := &pb.AssignPipelineRequest{
		ClientID:   client.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".rpc", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply pb.AssignPipelineReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	client.pipelines = append(client.pipelines, pipelineID)

	return nil
}

func (client *Client) RevokePipeline(pipelineID uint64) error {

	connection := client.controller.eventBus.bus.GetConnection()

	request := &pb.RevokePipelineRequest{
		ClientID:   client.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".rpc", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply pb.RevokePipelineReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (client *Client) ReleasePipeline(pipelineID uint64) bool {

	for idx, id := range client.pipelines {
		if id == pipelineID {
			// Remove pipeline
			client.pipelines = append(client.pipelines[:idx], client.pipelines[idx+1:]...)
			return true
		}
	}

	return false
}
