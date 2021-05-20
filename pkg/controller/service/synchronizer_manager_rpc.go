package controller

import (
	synchronizer_manager_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer_manager"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func (sm *SynchronizerManager) initialize_rpc() error {

	err := sm.initialize_rpc_register()
	if err != nil {
		return err
	}

	err = sm.initialize_rpc_unregister()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SynchronizerManager) initialize_rpc_register() error {

	connection := sm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.synchronizer_manager.register",
	}).Info("Subscribing to RPC channel")

	_, err := connection.Subscribe("gravity.synchronizer_manager.register", func(m *nats.Msg) {

		// Reply
		reply := synchronizer_manager_pb.RegisterSynchronizerReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req synchronizer_manager_pb.RegisterSynchronizerRequest
		err := proto.Unmarshal(m.Data, &req)
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
	})
	if err != nil {
		return err
	}

	return nil
}

func (sm *SynchronizerManager) initialize_rpc_unregister() error {

	connection := sm.controller.gravityClient.GetConnection()

	log.WithFields(log.Fields{
		"name": "gravity.synchronizer_manager.unregister",
	}).Info("Subscribing to RPC channel")

	_, err := connection.Subscribe("gravity.synchronizer_manager.unregister", func(m *nats.Msg) {

		// Reply
		reply := synchronizer_manager_pb.UnregisterSynchronizerReply{
			Success: true,
		}
		defer func() {
			data, _ := proto.Marshal(&reply)
			m.Respond(data)
		}()

		// Parsing request data
		var req synchronizer_manager_pb.UnregisterSynchronizerRequest
		err := proto.Unmarshal(m.Data, &req)
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
	})
	if err != nil {
		return err
	}

	return nil
}
