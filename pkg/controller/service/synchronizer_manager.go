package controller

import (
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

	err := sm.initialize_rpc()
	if err != nil {
		return err
	}

	return nil
}

func (sm *SynchronizerManager) Register(synchronizerID string) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.synchronizers[synchronizerID]
	if ok {
		return nil
	}

	// Create a new client
	synchronizer := NewSynchronizer(sm, synchronizerID)
	sm.synchronizers[synchronizerID] = synchronizer

	log.WithFields(log.Fields{
		"id": synchronizerID,
	}).Info("Registered synchronizer")

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
