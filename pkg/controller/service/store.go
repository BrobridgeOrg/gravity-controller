package controller

import (
	gravity_store "github.com/BrobridgeOrg/gravity-sdk/core/store"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func (controller *Controller) initializeStore() error {

	viper.SetDefault("controller.storePath", "./datastore")
	storePath := viper.GetString("controller.storePath")

	log.WithFields(log.Fields{
		"path": storePath,
	}).Info("Initializing store")

	options := gravity_store.NewOptions()
	options.StoreOptions.DatabasePath = storePath
	store, err := gravity_store.NewStore(options)
	if err != nil {
		return err
	}

	controller.store = store

	return nil
}
