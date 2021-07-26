package controller

import (
	"github.com/BrobridgeOrg/broc"
	authenticator "github.com/BrobridgeOrg/gravity-sdk/authenticator"
	"github.com/spf13/viper"
)

type Authentication struct {
	controller    *Controller
	channel       string
	accessKey     string
	authenticator *authenticator.Authenticator
	rpcEngine     *broc.Broc
}

func NewAuthentication() *Authentication {
	return &Authentication{}
}

func (auth *Authentication) Initialize(controller *Controller) error {

	auth.controller = controller

	// channel for authentication
	auth.channel = viper.GetString("auth_service.channel")

	// Initializing authenticator
	authOpts := authenticator.NewOptions()
	authOpts.Domain = controller.domain
	authOpts.Channel = auth.channel
	authOpts.AccessKey = viper.GetString("auth_service.accessKey")
	auth.authenticator = authenticator.NewAuthenticatorWithClient(controller.gravityClient, authOpts)

	// Initializing RPC handler
	return auth.InitializeRPC()
}
