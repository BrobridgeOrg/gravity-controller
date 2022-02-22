package rpc

import (
	"encoding/json"
	"fmt"

	adapter_sdk "github.com/BrobridgeOrg/gravity-sdk/adapter"
	"github.com/nats-io/nats.go"
)

type AdapterRPC struct {
	rpc    *RPC
	prefix string
	routes *Route
}

func NewAdapterRPC(rpc *RPC, prefix string) *AdapterRPC {

	a := &AdapterRPC{
		rpc: rpc,
	}

	// Preparing prefix
	a.prefix = fmt.Sprintf("%s.ADAPTER", prefix)

	a.initialize()

	return a
}

func (a *AdapterRPC) initialize() {
	a.routes = NewRoute(a.rpc, a.prefix)
	a.routes.Handle("REGISTER", a.Register)
}

func (a *AdapterRPC) Register(msg *nats.Msg) {

	var req adapter_sdk.RegisterRequest

	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	err, info := a.rpc.controller.RegisterAdapter(req.Name, req.Description)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	resp := adapter_sdk.RegisterReply{
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
