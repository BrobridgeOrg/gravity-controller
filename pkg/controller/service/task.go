package controller

type Task struct {
	Synchronizer *Synchronizer
	Pipeline     *Pipeline
}

func NewTask(syncronizer *Synchronizer, pipeline *Pipeline) *Task {
	return &Task{
		Synchronizer: syncronizer,
		Pipeline:     pipeline,
	}
}
