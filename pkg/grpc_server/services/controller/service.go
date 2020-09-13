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

	return &pb.GetClientCountReply{
		Count: 0,
	}, nil
}

func (service *Service) GetPipelineCount(ctx context.Context, in *pb.GetPipelineCountRequest) (*pb.GetPipelineCountReply, error) {

	return &pb.GetPipelineCountReply{
		Count: 0,
	}, nil
}

func (service *Service) Register(ctx context.Context, in *pb.RegisterRequest) (*pb.RegisterReply, error) {

	return &pb.RegisterReply{
		Success: true,
	}, nil
}

func (service *Service) ReleasePipelines(ctx context.Context, in *pb.ReleasePipelinesRequest) (*pb.ReleasePipelinesReply, error) {

	return &pb.ReleasePipelinesReply{
		Success: true,
	}, nil
}
