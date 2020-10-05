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

func (service *Service) GetPipelineCount(ctx context.Context, in *pb.GetPipelineCountRequest) (*pb.GetPipelineCountReply, error) {

	controller := service.app.GetController()

	return &pb.GetPipelineCountReply{
		Count: controller.GetPipelineCount(),
	}, nil
}

func (service *Service) Register(ctx context.Context, in *pb.RegisterRequest) (*pb.RegisterReply, error) {

	controller := service.app.GetController()

	err := controller.Register(in.ClientID)
	if err != nil {
		return &pb.RegisterReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	return &pb.RegisterReply{
		Success: true,
	}, nil
}

func (service *Service) Unregister(ctx context.Context, in *pb.UnregisterRequest) (*pb.UnregisterReply, error) {

	controller := service.app.GetController()

	err := controller.Unregister(in.ClientID)
	if err != nil {
		return &pb.UnregisterReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	return &pb.UnregisterReply{
		Success: true,
	}, nil
}

func (service *Service) ReleasePipelines(ctx context.Context, in *pb.ReleasePipelinesRequest) (*pb.ReleasePipelinesReply, error) {

	controller := service.app.GetController()

	var failures []uint64

	for _, pipelineID := range in.Pipelines {

		err := controller.ReleasePipeline(in.ClientID, pipelineID)
		if err != nil {
			failures = append(failures, pipelineID)
		}
	}

	if len(failures) > 0 {
		return &pb.ReleasePipelinesReply{
			Success:  false,
			Reason:   "Failed to release pipelines",
			Failures: failures,
		}, nil
	}

	return &pb.ReleasePipelinesReply{
		Success: true,
	}, nil
}

func (service *Service) GetPipelines(ctx context.Context, in *pb.GetPipelinesRequest) (*pb.GetPipelinesReply, error) {

	controller := service.app.GetController()

	pipelines, err := controller.GetPipelines(in.ClientID)
	if err != nil {
		return &pb.GetPipelinesReply{}, nil
	}

	return &pb.GetPipelinesReply{
		Pipelines: pipelines,
	}, nil
}
