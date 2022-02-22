package rpc

import (
	"context"
	"fmt"

	"github.com/BrobridgeOrg/gravity-controller/pkg/configs"
	"github.com/BrobridgeOrg/gravity-controller/pkg/connector"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

type RPC struct {
	controller *controller.Controller
	connector  *connector.Connector
	routes     *Route

	dispatcher *DispatcherRPC
	adapter    *AdapterRPC
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector, ctl *controller.Controller) *RPC {

	logger = l.Named("RPC")

	rpc := &RPC{
		controller: ctl,
		connector:  c,
	}

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return rpc.initialize()
			},
			OnStop: func(ctx context.Context) error {
				return nil
			},
		},
	)

	return rpc
}

func (rpc *RPC) initialize() error {

	// Preparing prefix
	prefix := fmt.Sprintf("$GRAVITY.%s.API", rpc.connector.GetDomain())

	logger.Info("Initializing RPC",
		zap.String("prefix", prefix),
	)

	rpc.dispatcher = NewDispatcherRPC(rpc, prefix)
	rpc.adapter = NewAdapterRPC(rpc, prefix)

	return nil
}
