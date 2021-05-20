package controller

import (
	"golang.org/x/net/context"

	pb "github.com/BrobridgeOrg/gravity-api/service/controller"
	app "github.com/BrobridgeOrg/gravity-controller/pkg/app"
)

type Service struct {
	app app.App
}

func NewService(a app.App) *Service {

	service := &Service{
		app: a,
	}

	return service
}

func (service *Service) GetClientCount(ctx context.Context, in *pb.GetClientCountRequest) (*pb.GetClientCountReply, error) {

	controller := service.app.GetController()

	return &pb.GetClientCountReply{
		Count: controller.GetClientCount(),
	}, nil
}

func (service *Service) RegisterAdapter(ctx context.Context, in *pb.RegisterAdapterRequest) (*pb.RegisterAdapterReply, error) {

	controller := service.app.GetController()

	err := controller.RegisterAdapter(in.ClientID)
	if err != nil {
		return &pb.RegisterAdapterReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	return &pb.RegisterAdapterReply{
		Success: true,
	}, nil
}

func (service *Service) UnregisterAdapter(ctx context.Context, in *pb.UnregisterAdapterRequest) (*pb.UnregisterAdapterReply, error) {

	controller := service.app.GetController()

	err := controller.UnregisterAdapter(in.ClientID)
	if err != nil {
		return &pb.UnregisterAdapterReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	return &pb.UnregisterAdapterReply{
		Success: true,
	}, nil
}
