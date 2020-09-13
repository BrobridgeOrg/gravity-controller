package instance

import (
	controller_service "github.com/BrobridgeOrg/gravity-controller/pkg/controller/service"
	grpc_server "github.com/BrobridgeOrg/gravity-controller/pkg/grpc_server/server"
	mux_manager "github.com/BrobridgeOrg/gravity-controller/pkg/mux_manager/manager"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done       chan bool
	muxManager *mux_manager.MuxManager
	grpcServer *grpc_server.Server
	grpcPool   *grpc_connection_pool.GRPCPool
	controller *controller_service.Controller
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	a.controller = controller_service.NewController(a)
	a.muxManager = mux_manager.NewMuxManager(a)
	a.grpcServer = grpc_server.NewServer(a)

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing gRPC pool
	err := a.initGRPCPool()
	if err != nil {
		return err
	}

	err = a.initController()
	if err != nil {
		return err
	}

	// Initializing GRPC server
	err = a.initGRPCServer()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	// GRPC
	go func() {
		err := a.runGRPCServer()
		if err != nil {
			log.Error(err)
		}
	}()

	err := a.runMuxManager()
	if err != nil {
		return err
	}

	<-a.done

	return nil
}
