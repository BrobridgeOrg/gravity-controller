package controller

type Task struct {
	Client   *Client
	Pipeline *Pipeline
}

func NewTask(client *Client, pipeline *Pipeline) *Task {
	return &Task{
		Client:   client,
		Pipeline: pipeline,
	}
}
