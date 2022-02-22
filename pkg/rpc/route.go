package rpc

import (
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Route struct {
	prefix string
	rpc    *RPC
}

func NewRoute(rpc *RPC, prefix string) *Route {
	return &Route{
		rpc:    rpc,
		prefix: prefix,
	}
}

func (r *Route) Handle(apiPath string, h func(*nats.Msg)) {

	fullPath := r.prefix + "." + apiPath

	logger.Info("Initialized RPC",
		zap.String("path", fullPath),
	)

	conn := r.rpc.connector.GetClient().GetConnection()
	conn.Subscribe(fullPath, h)
}
