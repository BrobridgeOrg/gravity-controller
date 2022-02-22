package rpc

import (
	"encoding/json"
	"fmt"

	dispatcher_sdk "github.com/BrobridgeOrg/gravity-sdk/dispatcher"
	"github.com/nats-io/nats.go"
)

type DispatcherRPC struct {
	rpc    *RPC
	prefix string
	routes *Route
}

func NewDispatcherRPC(rpc *RPC, prefix string) *DispatcherRPC {

	a := &DispatcherRPC{
		rpc: rpc,
	}

	// Preparing prefix
	a.prefix = fmt.Sprintf("%s.DISPATCHER", prefix)

	a.initialize()

	return a
}

func (a *DispatcherRPC) initialize() {
	a.routes = NewRoute(a.rpc, a.prefix)
	a.routes.Handle("REGISTER", a.Register)
}

func (a *DispatcherRPC) Register(msg *nats.Msg) {

	var req dispatcher_sdk.RegisterRequest

	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	err, info := a.rpc.controller.RegisterDispatcher(req.Name, req.Description)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	resp := dispatcher_sdk.RegisterReply{
		Name:        info.Name,
		Description: info.Description,
	}

	data, _ := json.Marshal(&resp)

	// Response
	err = msg.Respond(data)
	if err != nil {
		logger.Error(err.Error())
		return
	}
}
