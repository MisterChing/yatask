package yatask

import "time"

var (
	defaultWorkerNum int64 = 10
	defaultTick            = time.Second
	defaultTickSize  int64 = 10
)

type taskOptions struct {
	workerNum int64
	tick      time.Duration
	tickSize  int64
}

type TaskOption interface {
	apply(*taskOptions)
}

type workerNumConf struct {
	workerNum int64
}

func (w workerNumConf) apply(opts *taskOptions) {
	opts.workerNum = w.workerNum
}

func WithWorkerNum(num int64) TaskOption {
	return workerNumConf{
		workerNum: num,
	}
}

type tickConf struct {
	tick time.Duration
}

func (t tickConf) apply(opts *taskOptions) {
	opts.tick = t.tick
}

func WithTick(t time.Duration) TaskOption {
	return tickConf{
		tick: t,
	}
}

type tickSizeConf struct {
	tickSize int64
}

func (ts tickSizeConf) apply(opts *taskOptions) {
	opts.tickSize = ts.tickSize
}

func WithTickSize(size int64) TaskOption {
	return tickSizeConf{
		tickSize: size,
	}
}
