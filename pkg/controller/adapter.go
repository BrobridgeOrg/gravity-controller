package controller

import (
	adapter_sdk "github.com/BrobridgeOrg/gravity-sdk/adapter"
)

func (ctl *Controller) RegisterAdapter(name string, desc string) (error, *adapter_sdk.AdapterInfo) {

	// Getting adapter information from KV store
	entry, err := ctl.adapterConfigStore.Get(name)
	if err != nil {
		return err, nil
	}

	// Parsing
	var info adapter_sdk.AdapterInfo
	v := entry.Value()
	err = json.Unmarshal(v, &info)
	if err != nil {
		return err, nil
	}

	//TODO: check token and permissions
	info.Status = adapter_sdk.CONNECTED

	// Update
	data, _ := json.Marshal(info)
	_, err = ctl.adapterConfigStore.Put(name, data)
	if err != nil {
		return err, nil
	}

	return nil, &info
}
