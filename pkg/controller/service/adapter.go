package controller

import "encoding/json"

type Adapter struct {
	controller *Controller
	id         string
	name       string
	component  string
}

func NewAdapter(controller *Controller, component string, id string, name string) *Adapter {
	return &Adapter{
		controller: controller,
		id:         id,
		name:       name,
		component:  component,
	}
}

func (adapter *Adapter) save() error {

	// Update store
	store, err := adapter.controller.store.GetEngine().GetStore("gravity_adapter_manager")
	if err != nil {
		return nil
	}

	// Preparing JSON string
	data, err := json.Marshal(map[string]interface{}{
		"id":        adapter.id,
		"name":      adapter.name,
		"component": adapter.component,
	})
	if err != nil {
		return err
	}

	err = store.Put("adapters", []byte(adapter.id), data)
	if err != nil {
		return err
	}

	return nil
}

func (adapter *Adapter) release() error {

	// Update store
	store, err := adapter.controller.store.GetEngine().GetStore("gravity_adapter_manager")
	if err != nil {
		return nil
	}

	return store.Delete("adapters", []byte(adapter.id))
}
