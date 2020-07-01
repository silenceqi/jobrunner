package jobrunner

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Dispatcher struct {
	workerNums int
	workerPool chan *Worker
	jobPool    chan Job
	stopC      chan struct{}
	timeout    time.Duration //for worker config
	status     ProcessStatus
	stMutex    sync.Mutex
}

func NewDispatcher(workerNums int, options ...DispatcherOption) *Dispatcher {
	dpt := &Dispatcher{
		workerNums: workerNums,
		workerPool: make(chan *Worker, workerNums),
		jobPool:    make(chan Job),
		stopC:      make(chan struct{}),
	}
	for _, ops := range options {
		ops(dpt)
	}
	return dpt
}

func (dpt *Dispatcher) Run() <-chan struct{} {
	if status := dpt.GetStatus(); status == Running {
		panic("worker alreay running")
	}
	dpt.setStatus(Running)
	go func() {
	LOOP:
		for {
			select {
			case job := <-dpt.jobPool:
				wker := <-dpt.workerPool
				wker.Execute(job)
			case <-dpt.stopC:
				break LOOP
			}
		}
		log.Println("dispatcher stopped")
	}()
	var workerCompleteC []<-chan struct{}
	for i := 0; i < dpt.workerNums; i++ {
		options := []WorkerOption{
			WithName(fmt.Sprintf("%d", i+1)),
		}
		if int(dpt.timeout) > 0 {
			options = append(options, WithIdleTimeout(dpt.timeout))
		}
		worker := NewWorker(dpt, options...)
		workerCompleteC = append(workerCompleteC, worker.Run())
		dpt.workerPool <- worker
	}
	completeC := make(chan struct{})
	go func() {
		for _, cc := range workerCompleteC {
			<-cc
		}
		completeC <- struct{}{}
	}()

	return completeC

}

func (dpt *Dispatcher) Dispatch(job Job) {
	dpt.jobPool <- job
}

func (dpt *Dispatcher) Stop() {
	dpt.stopC <- struct{}{}
}
func (dpt *Dispatcher) GetStatus() ProcessStatus {
	dpt.stMutex.Lock()
	defer dpt.stMutex.Unlock()
	return dpt.status
}

func (dpt *Dispatcher) setStatus(status ProcessStatus) {
	dpt.stMutex.Lock()
	dpt.status = status
	dpt.stMutex.Unlock()
}

type DispatcherOption func(dpt *Dispatcher)

func WithTimeout(d time.Duration) DispatcherOption {
	return func(dpt *Dispatcher) {
		dpt.timeout = d
	}
}
