package main

import (
	"fmt"
	"net/http"
)

var (
	MaxWorker  = 10
	MaxQueue   = 10
	dispatcher *Dispatcher
	workers    []*Worker
)

/** 调度器 **/
type Dispatcher struct {
	WorkerPool chan chan Job
	JobQueue   chan Job
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		WorkerPool: make(chan chan Job, MaxWorker),
		JobQueue:   make(chan Job, MaxQueue),
	}
}

/** 初始化指定数量worker **/
func (d *Dispatcher) Launch() {
	for i := 0; i < MaxWorker; i++ {
		worker := NewWorker(d.WorkerPool, i)
		workers = append(workers, worker)
		worker.start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		job := <-d.JobQueue
		go func(job Job) {
			jobChannel := <-d.WorkerPool
			jobChannel <- job
		}(job)
	}
}

type Worker struct {
	No         int
	WorkerPool chan chan Job
	JobQueue   chan Job
	quitChan   chan bool
}

func NewWorker(WorkerPool chan chan Job, No int) *Worker {
	return &Worker{
		No:         No,
		WorkerPool: WorkerPool,
		JobQueue:   make(chan Job, MaxQueue),
		quitChan:   make(chan bool),
	}
}

func (w *Worker) start() {
	go func() {
		for {
			/** 自己加入到worker pool中 **/
			w.WorkerPool <- w.JobQueue
			select {
			case job := <-w.JobQueue:
				fmt.Printf("[%d]finished.[%s]\n", w.No, job.Payload)
			case <-w.quitChan:
				return
			}
		}
	}()
}

func (w *Worker) stop() {
	go func() {
		w.quitChan <- true
	}()
}

type Job struct {
	Payload []byte
}

func main() {
	dispatcher = NewDispatcher()
	dispatcher.Launch()
	http.HandleFunc("/worker", serve)
	http.ListenAndServe(":8123", nil)
}

func serve(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	dispatcher.JobQueue <- Job{Payload: []byte(query["name"][0])}
}
