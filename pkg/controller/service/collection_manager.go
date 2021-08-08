package controller

import (
	"time"

	"github.com/BrobridgeOrg/broc"
	"github.com/BrobridgeOrg/gravity-sdk/collection_manager/types"
	log "github.com/sirupsen/logrus"
)

type CollectionManager struct {
	controller *Controller
	rpcEngine  *broc.Broc
}

func NewCollectionManager(controller *Controller) *CollectionManager {
	return &CollectionManager{
		controller: controller,
	}
}

func (cm *CollectionManager) Initialize() error {

	store, err := cm.controller.store.GetEngine().GetStore("gravity_collection_manager")
	if err != nil {
		return err
	}

	err = store.RegisterColumns([]string{"collections"})
	if err != nil {
		return nil
	}

	// Initializing RPC
	err = cm.initializeRPC()
	if err != nil {
		return err
	}

	return nil
}

func (cm *CollectionManager) Register(collectionID string, collection *types.Collection) error {

	store, err := cm.controller.store.GetEngine().GetStore("gravity_collection_manager")
	if err != nil {
		return err
	}

	// Getting existing collection
	data, err := store.GetBytes("collections", []byte(collectionID))
	if err != nil {
		return err
	}

	// Doesn't exists
	if len(data) == 0 {
		collection.CreatedAt = time.Now()
		err := store.Put("collections", []byte(collectionID), collection.ToBytes())
		log.Info("Registered collection: %s", collection.ID)
		return err
	}

	return nil
}

func (cm *CollectionManager) Unregister(collectionID string) error {

	store, err := cm.controller.store.GetEngine().GetStore("gravity_collection_manager")
	if err != nil {
		return err
	}

	return store.Delete("collections", []byte(collectionID))
}

func (cm *CollectionManager) GetCollection(collectionID string) (*types.Collection, error) {

	store, err := cm.controller.store.GetEngine().GetStore("gravity_collection_manager")
	if err != nil {
		return nil, err
	}

	data, err := store.GetBytes("collections", []byte(collectionID))

	return types.Unmarshal(data)
}

func (cm *CollectionManager) GetCollections() ([]*types.Collection, error) {

	store, err := cm.controller.store.GetEngine().GetStore("gravity_collection_manager")
	if err != nil {
		return nil, err
	}

	collections := make([]*types.Collection, 0)
	err = store.List("collections", []byte(""), func(key []byte, value []byte) bool {
		collection, err := types.Unmarshal(value)
		if err != nil {
			log.Errorf("Unrecognized collection data: %s", string(key))
			return true
		}

		collections = append(collections, collection)

		return true
	})
	if err != nil {
		return nil, err
	}

	return collections, err
}
