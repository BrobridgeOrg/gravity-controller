package controller

type Service struct {
}

func NewService() *Service {
	return &Service{}
}

func (service *Service) Start() error {
	return nil
}
