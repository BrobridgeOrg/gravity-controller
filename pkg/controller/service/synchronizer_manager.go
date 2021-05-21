package controller

import (
	"encoding/json"
	"sync"

	log "github.com/sirupsen/logrus"
)

type SynchronizerManager struct {
	controller    *Controller
	synchronizers map[string]*Synchronizer
	mutex         sync.RWMutex
}

func NewSynchronizerManager(controller *Controller) *SynchronizerManager {
	return &SynchronizerManager{
		controller:    controller,
		synchronizers: make(map[string]*Synchronizer),
	}
}

func (sm *SynchronizerManager) Initialize() error {

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

	err = sm.initialize_rpc()
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

	return synchronizer.save()
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
