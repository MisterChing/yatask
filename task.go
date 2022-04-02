package yatask

import (
	"context"
	"errors"
	"log"
	"time"
)

type FutureItem struct {
	FutureTime time.Time
	value      interface{}
}

type loadFutureTaskFunc func() []FutureItem

type DelayTask struct {
	interval         time.Duration
	quitCh           chan struct{}
	workerNum        int64
	workerCh         chan struct{}
	delayQueue       *DelayQueue
	businessFn       func(ctx context.Context, task interface{})
	loadFutureTaskFn loadFutureTaskFunc
}

func NewDelayTask(opts ...TaskOption) *DelayTask {
	options := &taskOptions{
		workerNum: defaultWorkerNum,
		tick:      defaultTick,
		tickSize:  defaultTickSize,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	t := &DelayTask{
		quitCh:     make(chan struct{}),
		delayQueue: NewDelayQueue(options.tick, options.tickSize),
		workerNum:  options.workerNum,
		workerCh:   make(chan struct{}, options.workerNum),
	}
	return t
}

func (dt *DelayTask) SetBusinessFn(fn func(ctx context.Context, task interface{})) {
	dt.businessFn = fn
}

func (dt *DelayTask) SetLoadFutureTask(fn loadFutureTaskFunc) {
	dt.loadFutureTaskFn = fn
}

func (dt *DelayTask) PutTask(futureT time.Duration, value interface{}) {
	dt.delayQueue.ProduceMessage(futureT, value)
}

func (dt *DelayTask) workerIn() {
	if dt.workerNum > 0 {
		dt.workerCh <- struct{}{}
	}
}

func (dt *DelayTask) workerOut() {
	if dt.workerNum > 0 {
		<-dt.workerCh
	}
}

func (dt *DelayTask) Start() {
	//start time-wheel
	dt.delayQueue.tw.Start()
	//start load future data
	//go dt.doLoadFutureTask()
	//do work
	dt.doWork()
}

func (dt *DelayTask) Stop() {
	log.Println("start to stop")
	dt.delayQueue.tw.Stop()
	log.Println("time-wheel stopped")
	log.Println("start waiting in-processing tasks to be finished")
	//wait delayed message to be done
	for dt.delayQueue.messageDelayedNum > 0 || len(dt.workerCh) > 0 {
		log.Println("wait ...")
		time.Sleep(time.Second)
	}
	log.Println("all in-processing tasks is finished")
	dt.quitCh <- struct{}{}
	close(dt.quitCh)
	close(dt.workerCh)
	close(dt.delayQueue.messagesCh)
	close(dt.delayQueue.errors)
}

func (dt *DelayTask) doLoadFutureTask() {
	list := dt.loadFutureTaskFn()
	if len(list) > 0 {
		for _, v := range list {
			ft := v.FutureTime.Sub(time.Now())
			dt.PutTask(ft, v.value)
		}
	}
}

func (dt *DelayTask) doWork() {
	var stopLoop bool
	for !stopLoop {
		select {
		case message, ok := <-dt.delayQueue.Messages():
			if ok {
				dt.workerIn()
				go func() {
					defer func() {
						if err := recover(); err != nil {
							dt.delayQueue.errors <- errors.New("a panic error")
							dt.workerOut()
						}
					}()
					dt.businessFn(context.Background(), message)
					dt.workerOut()
				}()
			}
		case err, ok := <-dt.delayQueue.Errors():
			if ok {
				log.Println("hit error")
				log.Println(err)
			}
		case _, ok := <-dt.quitCh:
			if ok {
				log.Println("receive quit signal")
				stopLoop = true
			}
		}
	}
}
