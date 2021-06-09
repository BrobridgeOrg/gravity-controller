package controller

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	pb "github.com/BrobridgeOrg/gravity-api/service/adapter_manager"
)

func (am *AdapterManager) initialize_rpc() error {

	err := am.initialize_rpc_register()
	if err != nil {
		return err
	}

	return nil
}

func (am *AdapterManager) initialize_rpc_register() error {

	connection := am.controller.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.adapter_manager.register", am.controller.domain)

	log.WithFields(log.Fields{
		"name": channel,
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		// Reply
		reply := pb.RegisterAdapterReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req pb.RegisterAdapterRequest
		err := proto.Unmarshal(m.Data, &req)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = "UnknownParameter"
			return
		}

		err = am.Register(req.Component, req.AdapterID, req.Name)
		if err != nil {
			log.Error(err)

			reply.Success = false
			reply.Reason = err.Error()
			return
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (am *AdapterManager) initialize_rpc_unregister() error {

	connection := am.controller.gravityClient.GetConnection()
	channel := fmt.Sprintf("%s.adapter_manager.unregister", am.controller.domain)

	log.WithFields(log.Fields{
		"name": channel,
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe(channel, func(m *nats.Msg) {

		// Reply
		reply := pb.UnregisterAdapterReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req pb.UnregisterAdapterRequest
		err := proto.Unmarshal(m.Data, &req)
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
	})
	if err != nil {
		return err
	}

	return nil
}
