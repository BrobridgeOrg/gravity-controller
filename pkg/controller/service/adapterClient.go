package controller

import (
//"errors"
//"time"

//synchronizer "github.com/BrobridgeOrg/gravity-api/service/synchronizer"
//"github.com/golang/protobuf/proto"
)

type AdapterClient struct {
	controller *Controller
	id         string
	pipelines  []uint64
}

func NewAdapterClient(controller *Controller, id string) *AdapterClient {
	return &AdapterClient{
		controller: controller,
		id:         id,
	}
}
