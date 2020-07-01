package jobrunner

import (
	"fmt"
	"testing"
	"time"
)

type EasyJob struct {
	Name string
}

func (job *EasyJob) Run() {
	fmt.Printf("hello %s\n", job.Name)
}

func TestEasyJob(t *testing.T) {
	dpt := NewDispatcher(2, WithTimeout(2*time.Second))
	completeC := dpt.Run()
	for i := 0; i < 100; i++ {
		dpt.Dispatch(&EasyJob{fmt.Sprintf("job%d", i+1)})
	}
	<-completeC
}
