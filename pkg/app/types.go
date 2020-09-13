package app

import (
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller"
	"github.com/BrobridgeOrg/gravity-controller/pkg/grpc_server"
	"github.com/BrobridgeOrg/gravity-controller/pkg/mux_manager"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
)

type App interface {
	GetGRPCServer() grpc_server.Server
	GetMuxManager() mux_manager.Manager
	GetGRPCPool() *grpc_connection_pool.GRPCPool
	GetController() controller.Controller
}
