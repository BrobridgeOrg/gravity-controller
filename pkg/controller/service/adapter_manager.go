package controller

import (
	"encoding/json"
	"sync"

	"github.com/BrobridgeOrg/broc"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type AdapterManager struct {
	controller     *Controller
	allowAnonymous bool
	rpcEngine      *broc.Broc
	adapters       map[string]*Adapter
	mutex          sync.RWMutex
}

func NewAdapterManager(controller *Controller) *AdapterManager {
	return &AdapterManager{
		controller: controller,
		adapters:   make(map[string]*Adapter),
	}
}

func (am *AdapterManager) Initialize() error {

	// Load configurations
	viper.SetDefault("adapter_manager.allowAnonymous", true)
	am.allowAnonymous = viper.GetBool("adapter_manager.allowAnonymous")
	if am.allowAnonymous {
		key := am.controller.keyring.Get("anonymous")
		if key == nil {
			key = am.controller.keyring.Put("anonymous", "")
		}

		key.Permission().AddPermissions([]string{"ADAPTER"})
	}

	// Restore states from store
	store, err := am.controller.store.GetEngine().GetStore("gravity_adapter_manager")
	if err != nil {
		return err
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

	// Create a new adapter
	adapter = NewAdapter(am.controller, component, adapterID, name)
	am.adapters[adapterID] = adapter

	return adapter
}

func (am *AdapterManager) Register(component string, adapterID string, name string, key *keyring.KeyInfo) error {

	adapter := am.addAdapter(component, adapterID, name)

	adapter.save()

	// Update keyring to syncronizer
	am.controller.synchronizerManager.UpdateKeyring(key)

	log.WithFields(log.Fields{
		"component": component,
		"id":        adapterID,
		"name":      name,
	}).Info("Registered Adapter")

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

func (am *AdapterManager) GetAdapters() ([]*Adapter, error) {

	am.mutex.RLock()
	defer am.mutex.RUnlock()

	adapters := make([]*Adapter, 0, len(am.adapters))
	for _, adapter := range am.adapters {
		adapters = append(adapters, adapter)
	}

	return adapters, nil
}
