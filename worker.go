package jobrunner

import (
	"log"
	"sync"
	"time"
)

type ProcessStatus uint

const (
	Stoped ProcessStatus = iota
	Running
	Pending
)

type Worker struct {
	wc          chan Job
	stopC       chan struct{}
	name        string
	idleTimeout time.Duration
	status      ProcessStatus
	stMutex     *sync.Mutex
	dispatcher  *Dispatcher
}

func NewWorker(dpt *Dispatcher, options ...WorkerOption) *Worker {
	sc := make(chan struct{})
	wker := &Worker{
		stopC:       sc,
		wc:          make(chan Job),
		stMutex:     &sync.Mutex{},
		idleTimeout: time.Second * 30,
		dispatcher:  dpt,
	}
	for _, ops := range options {
		ops(wker)
	}
	return wker
}

type WorkerOption func(wker *Worker)

func WithName(name string) WorkerOption {
	return func(wker *Worker) {
		wker.name = name
	}
}

func WithIdleTimeout(d time.Duration) WorkerOption {
	return func(wker *Worker) {
		wker.idleTimeout = d
	}
}

func (wker *Worker) Run() <-chan struct{} {
	if status := wker.GetStatus(); status != Stoped {
		panic("worker alreay running")
	}
	log.Printf("running worker %s\n", wker.name)
	completeC := make(chan struct{})
	go func() {
		defer close(wker.wc)
	LOOP:
		for {
			wker.setStatus(Pending)
			select {
			case job := <-wker.wc:
				wker.setStatus(Running)
				job.Run()
				wker.dispatcher.workerPool <- wker
			case <-wker.stopC:
				break LOOP
			case <-time.After(wker.idleTimeout):
				break LOOP
			}
		}
		wker.setStatus(Stoped)
		log.Printf("woker %s is stopped", wker.name)
		completeC <- struct{}{}
	}()
	return completeC
}

func (wker *Worker) Execute(job Job) {
	wker.wc <- job
}

func (wker *Worker) Stop() {
	wker.stopC <- struct{}{}
}

func (wker *Worker) GetStatus() ProcessStatus {
	wker.stMutex.Lock()
	defer wker.stMutex.Unlock()
	return wker.status
}

func (wker *Worker) setStatus(status ProcessStatus) {
	wker.stMutex.Lock()
	wker.status = status
	wker.stMutex.Unlock()
}
