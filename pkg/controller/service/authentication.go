package controller

import (
	"github.com/BrobridgeOrg/broc"
	authenticator "github.com/BrobridgeOrg/gravity-sdk/authenticator"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/spf13/viper"
)

type Authentication struct {
	controller         *Controller
	enabledAuthService bool
	channel            string
	accessKey          string
	authenticator      *authenticator.Authenticator
	rpcEngine          *broc.Broc
}

func NewAuthentication() *Authentication {
	return &Authentication{}
}

func (auth *Authentication) Initialize(controller *Controller) error {

	auth.controller = controller

	viper.SetDefault("auth_service.enabled", false)
	auth.enabledAuthService = viper.GetBool("auth_service.enabled")

	// channel for authentication
	auth.channel = viper.GetString("auth_service.channel")

	// Initializing authenticator
	authOpts := authenticator.NewOptions()
	authOpts.Domain = controller.domain
	authOpts.Channel = auth.channel
	authOpts.Key = keyring.NewKey("gravity", viper.GetString("auth_service.accessKey"))
	auth.authenticator = authenticator.NewAuthenticatorWithClient(controller.gravityClient, authOpts)

	// Initializing RPC handler
	return auth.InitializeRPC()
}

func (auth *Authentication) Authenticate(appID string, token []byte, allowAnonymous bool) *keyring.KeyInfo {

	if allowAnonymous {

		// Allow anonymous subscriber
		key := auth.controller.keyring.Get("anonymous")
		if key != nil {
			return key
		}
	}

	if auth.enabledAuthService {

		// Authenticate application and token
		entity, err := auth.authenticator.Authenticate(appID, token)
		if err != nil {
			return nil
		}

		// Add to keyring
		key := auth.controller.keyring.Put(entity.AppID, entity.AccessKey)

		// Load permissions
		if v, ok := entity.Properties["permissions"]; ok {
			key.Permission().AddPermissions(v.([]string))
		}

		return key
	}

	return nil
}
