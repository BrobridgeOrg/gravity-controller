package controller

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

type AdapterManager struct {
	controller *Controller
	adapters   map[string]*Adapter
	mutex      sync.RWMutex
}

func NewAdapterManager(controller *Controller) *AdapterManager {
	return &AdapterManager{
		controller: controller,
		adapters:   make(map[string]*Adapter),
	}
}

func (am *AdapterManager) Initialize() error {

	err := am.initialize_rpc()
	if err != nil {
		return err
	}

	return nil
}

func (am *AdapterManager) Register(component string, adapterID string, name string) error {

	am.mutex.Lock()
	defer am.mutex.Unlock()

	_, ok := am.adapters[adapterID]
	if ok {
		return nil
	}

	// Create a new synchronizer
	adapter := NewAdapter(am.controller, component, adapterID, name)
	am.adapters[adapterID] = adapter

	log.WithFields(log.Fields{
		"id": adapter,
	}).Info("Registered Adapter")

	return nil
}

func (am *AdapterManager) Unregister(adapterID string) error {

	am.mutex.Lock()
	defer am.mutex.Unlock()

	// Take off synchronizer from registry
	delete(am.adapters, adapterID)

	return nil
}
