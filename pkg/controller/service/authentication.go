package controller

import (
	authenticator "github.com/BrobridgeOrg/gravity-sdk/authenticator"
	"github.com/spf13/viper"
)

type Authentication struct {
	channel       string
	accessKey     string
	authenticator *authenticator.Authenticator
}

func NewAuthentication() *Authentication {
	return &Authentication{}
}

func (auth *Authentication) Initialize(controller *Controller) error {

	// channel for authentication
	auth.channel = viper.GetString("auth_service.channel")

	// Initializing authenticator
	authOpts := authenticator.NewOptions()
	authOpts.Domain = controller.domain
	authOpts.Channel = auth.channel
	authOpts.AccessKey = viper.GetString("auth_service.accessKey")
	auth.authenticator = authenticator.NewAuthenticatorWithClient(controller.gravityClient, authOpts)

	return nil
}
