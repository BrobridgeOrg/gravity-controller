package controller

import (
	"encoding/json"
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

	// Restore states from store
	store, err := am.controller.store.GetEngine().GetStore("gravity_adapter_manager")
	if err != nil {
		return nil
	}

	err = store.RegisterColumns([]string{"adapters"})
	if err != nil {
		return nil
	}

	log.Info("Trying to restoring adapters...")

	store.List("adapters", []byte(""), func(key []byte, value []byte) bool {

		var data map[string]interface{}
		json.Unmarshal(value, &data)

		adapter := am.addAdapter(
			data["component"].(string),
			data["id"].(string),
			data["name"].(string),
		)

		log.WithFields(log.Fields{
			"id":        adapter.id,
			"name":      adapter.name,
			"component": adapter.component,
		}).Info("Restored adapter")

		return true
	})

	err = am.initialize_rpc()
	if err != nil {
		return err
	}

	return nil
}

func (am *AdapterManager) addAdapter(component string, adapterID string, name string) *Adapter {

	am.mutex.Lock()
	defer am.mutex.Unlock()

	adapter, ok := am.adapters[adapterID]
	if ok {
		return adapter
	}

	// Create a new synchronizer
	adapter = NewAdapter(am.controller, component, adapterID, name)
	am.adapters[adapterID] = adapter

	return adapter
}

func (am *AdapterManager) Register(component string, adapterID string, name string) error {

	adapter := am.addAdapter(component, adapterID, name)

	log.WithFields(log.Fields{
		"component": component,
		"id":        adapterID,
		"name":      name,
	}).Info("Registered Adapter")

	adapter.save()

	return nil
}

func (am *AdapterManager) Unregister(adapterID string) error {

	am.mutex.Lock()
	defer am.mutex.Unlock()

	// Release
	adapter, ok := am.adapters[adapterID]
	if !ok {
		return nil
	}

	adapter.release()

	// Remove adapter from registry
	delete(am.adapters, adapterID)

	return nil
}
