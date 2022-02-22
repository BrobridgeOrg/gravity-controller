package controller

import (
	dispatcher_sdk "github.com/BrobridgeOrg/gravity-sdk/dispatcher"
)

func (ctl *Controller) RegisterDispatcher(name string, desc string) (error, *dispatcher_sdk.DispatcherInfo) {

	// Getting dispatcher information from KV store
	entry, err := ctl.dispatcherConfigStore.Get(name)
	if err != nil {
		return err, nil
	}

	// Parsing
	var info dispatcher_sdk.DispatcherInfo
	v := entry.Value()
	err = json.Unmarshal(v, &info)
	if err != nil {
		return err, nil
	}

	//TODO: check token and permissions
	info.Status = dispatcher_sdk.CONNECTED

	// Update
	data, _ := json.Marshal(info)
	_, err = ctl.dispatcherConfigStore.Put(name, data)
	if err != nil {
		return err, nil
	}

	return nil, &info
}
