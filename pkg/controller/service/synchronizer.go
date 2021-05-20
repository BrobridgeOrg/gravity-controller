package controller

import (
	"errors"
	"time"

	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
)

type Synchronizer struct {
	synchronizerManager *SynchronizerManager
	id                  string
	pipelines           []uint64
}

func NewSynchronizer(sm *SynchronizerManager, id string) *Synchronizer {
	return &Synchronizer{
		synchronizerManager: sm,
		id:                  id,
		pipelines:           make([]uint64, 0),
	}
}

func (synchronizer *Synchronizer) getConnection() *nats.Conn {
	return synchronizer.synchronizerManager.controller.gravityClient.GetConnection()
}

func (synchronizer *Synchronizer) AssignPipeline(pipelineID uint64) error {

	connection := synchronizer.getConnection()

	request := &synchronizer_pb.AssignPipelineRequest{
		ClientID:   synchronizer.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	response, err := connection.Request("gravity.eventstore."+synchronizer.id+".AssignPipeline", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.AssignPipelineReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	synchronizer.pipelines = append(synchronizer.pipelines, pipelineID)

	return nil
}

func (synchronizer *Synchronizer) RevokePipeline(pipelineID uint64) error {

	connection := synchronizer.getConnection()

	request := &synchronizer_pb.RevokePipelineRequest{
		ClientID:   synchronizer.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	response, err := connection.Request("gravity.eventstore."+synchronizer.id+".RevokePipeline", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.RevokePipelineReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (synchronizer *Synchronizer) ReleasePipeline(pipelineID uint64) bool {

	for idx, id := range synchronizer.pipelines {
		if id == pipelineID {
			// Remove pipeline
			synchronizer.pipelines = append(synchronizer.pipelines[:idx], synchronizer.pipelines[idx+1:]...)
			return true
		}
	}

	return false
}

func (synchronizer *Synchronizer) RegisterSubscriber(subscriberID string) error {

	connection := synchronizer.getConnection()

	request := &synchronizer_pb.RegisterSubscriberRequest{
		SubscriberID: subscriberID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	response, err := connection.Request("gravity.eventstore."+synchronizer.id+".RegisterSubscriber", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.RegisterSubscriberReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (synchronizer *Synchronizer) UnregisterSubscriber(subscriberID string) error {

	connection := synchronizer.getConnection()

	request := &synchronizer_pb.UnregisterSubscriberRequest{
		SubscriberID: subscriberID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	response, err := connection.Request("gravity.eventstore."+synchronizer.id+".UnregisterSubscriber", data, time.Second*5)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.UnregisterSubscriberReply
	err = proto.Unmarshal(response.Data, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}
