package controller

import (
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

	log.WithFields(log.Fields{
		"name": "gravity.adapter_manager.register",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.adapter_manager.register", func(m *nats.Msg) {

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

	log.WithFields(log.Fields{
		"name": "gravity.adapter_manager.unregister",
	}).Info("Subscribing to channel")

	_, err := connection.Subscribe("gravity.adapter_manager.unregister", func(m *nats.Msg) {

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
