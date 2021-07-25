package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
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

func (synchronizer *Synchronizer) request(eventstoreID string, method string, data []byte, encrypted bool) ([]byte, error) {

	conn := synchronizer.synchronizerManager.controller.gravityClient.GetConnection()

	// Preparing packet
	packet := packet_pb.Packet{
		AppID:   "gravity",
		Payload: data,
	}

	// find the key for gravity
	keyInfo := synchronizer.synchronizerManager.controller.keyring.Get("gravity")
	if keyInfo == nil {
		return []byte(""), errors.New("No access key for gravity")
	}

	// Encrypt
	if encrypted {
		payload, err := keyInfo.Encryption().Encrypt(data)
		if err != nil {
			return []byte(""), err
		}

		packet.Payload = payload
	}

	msg, _ := proto.Marshal(&packet)

	// Send request
	channel := fmt.Sprintf("%s.eventstore.%s.%s", synchronizer.synchronizerManager.controller.domain, eventstoreID, method)
	resp, err := conn.Request(channel, msg, time.Second*10)
	if err != nil {
		return []byte(""), err
	}

	// Decrypt
	if encrypted {
		data, err = keyInfo.Encryption().Decrypt(resp.Data)
		if err != nil {
			return []byte(""), err
		}

		return data, nil
	}

	return resp.Data, nil
}

func (synchronizer *Synchronizer) save() error {

	// Update store
	store, err := synchronizer.synchronizerManager.controller.store.GetEngine().GetStore("gravity_synchronizer_manager")
	if err != nil {
		return nil
	}

	// Preparing JSON string
	data, err := json.Marshal(map[string]interface{}{
		"id":        synchronizer.id,
		"pipelines": synchronizer.pipelines,
	})
	if err != nil {
		return err
	}

	err = store.Put("synchronizers", []byte(synchronizer.id), data)
	if err != nil {
		return err
	}

	return nil
}

func (synchronizer *Synchronizer) getConnection() *nats.Conn {
	return synchronizer.synchronizerManager.controller.gravityClient.GetConnection()
}

func (synchronizer *Synchronizer) AssignPipeline(pipelineID uint64) error {

	request := &synchronizer_pb.AssignPipelineRequest{
		ClientID:   synchronizer.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	respData, err := synchronizer.request(synchronizer.id, "assignPipeline", data, true)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.AssignPipelineReply
	err = proto.Unmarshal(respData, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	synchronizer.pipelines = append(synchronizer.pipelines, pipelineID)
	synchronizer.save()

	return nil
}

func (synchronizer *Synchronizer) RevokePipeline(pipelineID uint64) error {

	request := &synchronizer_pb.RevokePipelineRequest{
		ClientID:   synchronizer.id,
		PipelineID: pipelineID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	respData, err := synchronizer.request(synchronizer.id, "revokePipeline", data, true)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.RevokePipelineReply
	err = proto.Unmarshal(respData, &reply)
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
			synchronizer.save()
			return true
		}
	}

	return false
}

func (synchronizer *Synchronizer) RegisterSubscriber(subscriberID string) error {

	request := &synchronizer_pb.RegisterSubscriberRequest{
		SubscriberID: subscriberID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	respData, err := synchronizer.request(synchronizer.id, "registerSubscriber", data, true)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.RegisterSubscriberReply
	err = proto.Unmarshal(respData, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}

func (synchronizer *Synchronizer) UnregisterSubscriber(subscriberID string) error {

	request := &synchronizer_pb.UnregisterSubscriberRequest{
		SubscriberID: subscriberID,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	// Send request to synchronizer
	respData, err := synchronizer.request(synchronizer.id, "unregisterSubscriber", data, true)
	if err != nil {
		return err
	}

	var reply synchronizer_pb.UnregisterSubscriberReply
	err = proto.Unmarshal(respData, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New(reply.Reason)
	}

	return nil
}
