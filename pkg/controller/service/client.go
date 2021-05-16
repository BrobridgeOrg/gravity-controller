package controller

import (
	"errors"
	"time"

	synchronizer "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
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

	request := &synchronizer.AssignPipelineRequest{
		ClientID:   client.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".AssignPipeline", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer.AssignPipelineReply
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

	request := &synchronizer.RevokePipelineRequest{
		ClientID:   client.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".RevokePipeline", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer.RevokePipelineReply
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

func (client *Client) RegisterSubscriber(transmitterID string) error {

	connection := client.controller.eventBus.bus.GetConnection()

	request := &synchronizer.RegisterSubscriberRequest{
		SubscriberID: transmitterID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".RegisterSubscriber", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer.RegisterSubscriberReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (client *Client) UnregisterSubscriber(transmitterID string) error {

	connection := client.controller.eventBus.bus.GetConnection()

	request := &synchronizer.UnregisterSubscriberRequest{
		SubscriberID: transmitterID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".UnregisterSubscriber", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer.UnregisterSubscriberReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

/*
func (client *Client) StartSubscriber(transmitterID string) error {

	connection := client.controller.eventBus.bus.GetConnection()

	request := &synchronizer.StartSubscriberRequest{
		SubscriberID: transmitterID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to client
	response, err := connection.Request("gravity.eventstore."+client.id+".StartSubscriber", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer.StartSubscriberReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}
*/
