package controller

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/BrobridgeOrg/broc"
	synchronizer_pb "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/BrobridgeOrg/gravity-sdk/eventstore"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type SynchronizerManager struct {
	controller    *Controller
	synchronizers map[string]*Synchronizer
	eventstore    *eventstore.EventStore
	rpcEngine     *broc.Broc
	mutex         sync.RWMutex
}

func NewSynchronizerManager(controller *Controller) *SynchronizerManager {
	return &SynchronizerManager{
		controller:    controller,
		synchronizers: make(map[string]*Synchronizer),
	}
}

func (sm *SynchronizerManager) Initialize() error {

	// Initializing eventstore
	authOpts := eventstore.NewOptions()
	authOpts.Domain = sm.controller.domain
	authOpts.Key = sm.controller.keyring.Get("gravity")
	sm.eventstore = eventstore.NewEventStoreWithClient(sm.controller.gravityClient, authOpts)

	// Restore states from store
	store, err := sm.controller.store.GetEngine().GetStore("gravity_synchronizer_manager")
	if err != nil {
		return nil
	}

	err = store.RegisterColumns([]string{"synchronizers"})
	if err != nil {
		return nil
	}

	log.Info("Trying to restoring synchronizers...")

	store.List("synchronizers", []byte(""), func(key []byte, value []byte) bool {

		var data map[string]interface{}
		json.Unmarshal(value, &data)

		synchronizer, err := sm.addSynchronizer(
			data["id"].(string),
		)
		if err != nil {
			log.Error(err)
			return false
		}

		log.WithFields(log.Fields{
			"id": synchronizer.id,
		}).Info("Restored synchronizer")

		// Update pipelines
		if data["pipelines"] != nil {
			ps := data["pipelines"].([]interface{})

			for _, p := range ps {
				if p == nil {
					continue
				}

				pipelineID := uint64(p.(float64))
				synchronizer.pipelines = append(synchronizer.pipelines, pipelineID)
				sm.controller.pipelineManager.addPipeline(pipelineID, synchronizer.id)
			}
		}

		return true
	})

	err = sm.initializeRPC()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SynchronizerManager) addSynchronizer(synchronizerID string) (*Synchronizer, error) {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	synchronizer, ok := sm.synchronizers[synchronizerID]
	if ok {
		return synchronizer, nil
	}

	// Create a new client
	synchronizer = NewSynchronizer(sm, synchronizerID)
	sm.synchronizers[synchronizerID] = synchronizer

	return synchronizer, nil
}

func (sm *SynchronizerManager) Register(synchronizerID string) error {

	synchronizer, err := sm.addSynchronizer(synchronizerID)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"id": synchronizerID,
	}).Info("Registered synchronizer")

	err = synchronizer.save()
	if err != nil {
		return err
	}

	// Update keyring
	keys := sm.controller.keyring.GetKeys()
	keys.Range(func(k interface{}, v interface{}) bool {
		key := v.(*keyring.KeyInfo)
		log.WithFields(log.Fields{
			"id":           key.GetAppID(),
			"synchronizer": synchronizerID,
		}).Info("Update key to synchronizer")

		sm.UpdateKeyringBySynchronizer(synchronizerID, key)
		return true
	})

	return nil
}

func (sm *SynchronizerManager) Unregister(synchronizerID string) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	synchronizer, ok := sm.synchronizers[synchronizerID]
	if ok {
		return nil
	}

	// Release pipelines
	for _, pipelineID := range synchronizer.pipelines {

		// Getting pipeline by ID
		pipeline := sm.controller.pipelineManager.GetPipeline(pipelineID)
		if pipeline == nil {
			return nil
		}

		sm.controller.pipelineManager.releasePipeline(pipeline)
	}

	// Take off synchronizer from registry
	delete(sm.synchronizers, synchronizerID)

	return nil
}

func (sm *SynchronizerManager) GetCount() int {
	return len(sm.synchronizers)
}

func (sm *SynchronizerManager) GetSynchronizers() map[string]*Synchronizer {
	return sm.synchronizers
}

func (sm *SynchronizerManager) GetSynchronizer(synchronizerID string) *Synchronizer {

	synchronizer, ok := sm.synchronizers[synchronizerID]
	if !ok {
		return nil
	}

	return synchronizer
}

func (sm *SynchronizerManager) Request(synchronizerID string, method string, data []byte) ([]byte, error) {
	return sm.eventstore.Request(synchronizerID, method, data)
}

func (sm *SynchronizerManager) UpdateKeyring(key *keyring.KeyInfo) error {

	for synchronizerID, _ := range sm.GetSynchronizers() {

		err := sm.UpdateKeyringBySynchronizer(synchronizerID, key)
		if err != nil {
			continue
		}
	}

	return nil
}

func (sm *SynchronizerManager) UpdateKeyringBySynchronizer(synchronizerID string, key *keyring.KeyInfo) error {

	request := synchronizer_pb.UpdateKeyringRequest{
		Keys: make([]*synchronizer_pb.Key, 1),
	}

	entry := synchronizer_pb.Key{
		AppID:       key.GetAppID(),
		Key:         key.Encryption().GetKey(),
		Permissions: key.Permission().GetPermissions(),
	}

	request.Keys[0] = &entry

	msg, _ := proto.Marshal(&request)

	respData, err := sm.Request(synchronizerID, "updateKeyring", msg)
	if err != nil {
		log.Error(err)
		return err
	}

	var reply synchronizer_pb.UpdateKeyringReply
	err = proto.Unmarshal(respData, &reply)
	if err != nil {
		log.Error(err)
		return err
	}

	if !reply.Success {
		log.Error(reply.Reason)
		return errors.New(reply.Reason)
	}

	return nil
}
