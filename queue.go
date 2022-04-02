package yatask

import (
	"sync/atomic"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/google/uuid"
	cmap "github.com/orcaman/concurrent-map"
)

type DelayQueue struct {
	// time-wheel instance
	tw *timingwheel.TimingWheel

	// taskPool
	taskPool          cmap.ConcurrentMap
	messageDelayedNum int64

	messagesCh chan interface{}
	errors     chan error
}

func NewDelayQueue(tick time.Duration, wheelSize int64) *DelayQueue {
	q := &DelayQueue{
		taskPool:   cmap.New(),
		messagesCh: make(chan interface{}),
		errors:     make(chan error),
	}
	q.tw = timingwheel.NewTimingWheel(tick, wheelSize)
	return q
}

func (queue *DelayQueue) Errors() <-chan error { return queue.errors }

func (queue *DelayQueue) Messages() <-chan interface{} { return queue.messagesCh }

func (queue *DelayQueue) ProduceMessage(futureT time.Duration, value interface{}) {
	key := uuid.New().String()
	queue.taskPool.Set(key, value)
	queue.tw.AfterFunc(futureT, func() { //every func is a goroutine
		if v, ok := queue.taskPool.Get(key); ok {
			atomic.AddInt64(&queue.messageDelayedNum, 1)
			queue.messagesCh <- v //may be blocked, cause task losses & goroutine accumulate, need to wait m.Count() == 0, then exit.
			queue.taskPool.Remove(key)
			atomic.AddInt64(&queue.messageDelayedNum, -1)
		}
	})
}
