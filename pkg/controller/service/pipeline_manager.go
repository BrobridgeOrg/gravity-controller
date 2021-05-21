package controller

import (
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type PipelineManager struct {
	controller   *Controller
	pipelines    map[uint64]*Pipeline
	pendingTasks chan *Task
}

func NewPipelineManager(controller *Controller) *PipelineManager {
	return &PipelineManager{
		controller: controller,
		pipelines:  make(map[uint64]*Pipeline),
	}
}

func (pm *PipelineManager) Initialize() error {

	// Initializing pipelines
	viper.SetDefault("controller.pipelineCount", 256)
	pipelineCount := viper.GetUint64("controller.pipelineCount")
	pm.pendingTasks = make(chan *Task, pipelineCount)
	for i := uint64(0); i < pipelineCount; i++ {

		if _, ok := pm.pipelines[i]; ok {
			continue
		}

		pipeline := pm.addPipeline(i, "")
		pm.pendingTasks <- NewTask(nil, pipeline)
	}

	go pm.watchTasks()

	// Initializing RPC
	err := pm.initialize_rpc()
	if err != nil {
		return err
	}

	return nil
}

func (pm *PipelineManager) watchTasks() {

	for {
		select {
		case task := <-pm.pendingTasks:
			success := pm.HandleTask(task)
			if !success {
				// Failed to process this task so we handle it later
				pm.pendingTasks <- task
			}
		}
	}
}

func (pm *PipelineManager) addPipeline(pipelineID uint64, synchronizerID string) *Pipeline {
	pipeline := NewPipeline(pipelineID)
	pipeline.Assign(synchronizerID)
	pm.pipelines[pipelineID] = pipeline

	return pipeline
}

func (pm *PipelineManager) assignPipeline(synchronizer *Synchronizer, pipeline *Pipeline) error {

	err := synchronizer.AssignPipeline(pipeline.id)
	if err != nil {
		return err
	}

	pipeline.Assign(synchronizer.id)

	return nil
}

func (pm *PipelineManager) releasePipeline(pipeline *Pipeline) error {

	// Release pipeline back to pool
	pipeline.Release()
	pm.pendingTasks <- NewTask(nil, pipeline)

	return nil
}

func (pm *PipelineManager) HandleTask(task *Task) bool {

	if task.Synchronizer == nil {
		return pm.DispatchPipeline(task.Pipeline)
	}

	// Re-assign pipeline to specific client
	err := pm.assignPipeline(task.Synchronizer, task.Pipeline)
	if err != nil {
		return false
	}

	return true
}

func (pm *PipelineManager) DispatchPipeline(pipeline *Pipeline) bool {

	var found *Synchronizer

	clients := pm.controller.synchronizerManager.GetSynchronizers()

	// Find a client to assign pipeline
	for _, client := range clients {
		if found == nil {
			found = client
			continue
		}

		if len(found.pipelines) > len(client.pipelines) {
			found = client
		}
	}

	if found == nil {
		return false
	}

	log.WithFields(log.Fields{
		"pipeline": pipeline.id,
		"client":   found.id,
	}).Info("Assigning pipeline")

	timer := time.NewTimer(10 * time.Millisecond)
	<-timer.C
	// Assign pipeline to client
	err := found.AssignPipeline(pipeline.id)
	if err != nil {
		return false
	}

	return true
}

func (pm *PipelineManager) AssignPipeline(synchronizerID string, pipelineID uint64) error {

	synchronizer := pm.controller.synchronizerManager.GetSynchronizer(synchronizerID)
	if synchronizer == nil {
		return errors.New("No such synchronizer: " + synchronizerID)
	}

	pipeline := pm.GetPipeline(pipelineID)
	if pipeline == nil {
		return errors.New("No such pipeline: " + fmt.Sprintf("%d", pipelineID))
	}

	return pm.assignPipeline(synchronizer, pipeline)
}

func (pm *PipelineManager) ReleasePipeline(synchronizerID string, pipelineID uint64) error {

	synchronizer := pm.controller.synchronizerManager.GetSynchronizer(synchronizerID)
	if synchronizer == nil {
		return nil
	}

	if !synchronizer.ReleasePipeline(pipelineID) {
		return nil
	}

	pipeline := pm.GetPipeline(pipelineID)
	if pipeline == nil {
		return nil
	}

	return pm.releasePipeline(pipeline)
}

func (pm *PipelineManager) RevokePipeline(synchronizerID string, pipelineID uint64) error {

	synchronizer := pm.controller.synchronizerManager.GetSynchronizer(synchronizerID)
	if synchronizer == nil {
		return errors.New("No such synchronizer: " + synchronizerID)
	}

	pipeline := pm.GetPipeline(pipelineID)
	if pipeline == nil {
		return errors.New("No such pipeline: " + fmt.Sprintf("%d", pipelineID))
	}

	return synchronizer.RevokePipeline(pipeline.id)
}

func (pm *PipelineManager) GetCount() int {
	return len(pm.pipelines)
}

func (pm *PipelineManager) GetPipeline(pipelineID uint64) *Pipeline {

	pipeline, ok := pm.pipelines[pipelineID]
	if !ok {
		return nil
	}

	return pipeline
}
