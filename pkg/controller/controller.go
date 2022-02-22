package controller

import (
	"context"

	"github.com/BrobridgeOrg/gravity-controller/pkg/configs"
	"github.com/BrobridgeOrg/gravity-controller/pkg/connector"
	"github.com/BrobridgeOrg/gravity-sdk/config_store"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var logger *zap.Logger

type Controller struct {
	config                *configs.Config
	connector             *connector.Connector
	dispatcherConfigStore *config_store.ConfigStore
	snapshotConfigStore   *config_store.ConfigStore
	adapterConfigStore    *config_store.ConfigStore
	subscriberConfigStore *config_store.ConfigStore
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector) *Controller {

	logger = l.Named("Controller")

	ctl := &Controller{
		config:    config,
		connector: c,
	}

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return ctl.initialize()
			},
			OnStop: func(ctx context.Context) error {
				return nil
			},
		},
	)

	return ctl
}

func (ctl *Controller) initialize() error {

	logger.Info(
		"Initializing config store",
		zap.String("component", "dispatcher"),
	)

	ctl.dispatcherConfigStore = config_store.NewConfigStore(ctl.connector.GetClient(),
		config_store.WithDomain(ctl.connector.GetDomain()),
		config_store.WithCatalog("DISPATCHER"),
	)

	err := ctl.dispatcherConfigStore.Init()
	if err != nil {
		return err
	}

	logger.Info(
		"Initializing config store",
		zap.String("component", "snapshot"),
	)

	ctl.snapshotConfigStore = config_store.NewConfigStore(ctl.connector.GetClient(),
		config_store.WithDomain(ctl.connector.GetDomain()),
		config_store.WithCatalog("SNAPSHOT"),
	)

	err = ctl.snapshotConfigStore.Init()
	if err != nil {
		return err
	}

	logger.Info(
		"Initializing config store",
		zap.String("component", "adapter"),
	)

	ctl.adapterConfigStore = config_store.NewConfigStore(ctl.connector.GetClient(),
		config_store.WithDomain(ctl.connector.GetDomain()),
		config_store.WithCatalog("ADAPTER"),
	)

	err = ctl.adapterConfigStore.Init()
	if err != nil {
		return err
	}

	logger.Info(
		"Initializing config store",
		zap.String("component", "subscriber"),
	)

	ctl.subscriberConfigStore = config_store.NewConfigStore(ctl.connector.GetClient(),
		config_store.WithDomain(ctl.connector.GetDomain()),
		config_store.WithCatalog("SUBSCRIBER"),
	)

	err = ctl.subscriberConfigStore.Init()
	if err != nil {
		return err
	}

	return nil
}
