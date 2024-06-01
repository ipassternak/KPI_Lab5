package datastore

import (
	"fmt"
	"sync"
	"time"
)

var ErrWorkerQueueIsClosed = fmt.Errorf("worker queue is closed")

type getResult struct {
	value string
	err   error
}

type getMsg struct {
	key   string
	resCh chan getResult
}

type worker func(string) (string, error)

type workerQueue struct {
	workerPool []chan getMsg
	msgQueue   chan getMsg

	mu sync.Mutex

	isClosed bool
}

func newWorkerQueue(w worker, workerCount int) *workerQueue {
	q := &workerQueue{
		workerPool: make([]chan getMsg, workerCount),
	}
	for i := 0; i < workerCount; i++ {
		q.workerPool[i] = make(chan getMsg)
		go q.spawnWorker(w, q.workerPool[i])
	}
	q.start(workerCount)
	return q
}

func (q *workerQueue) start(workerCount int) {
	q.msgQueue = make(chan getMsg, workerCount)
	go func() {
		for msg := range q.msgQueue {
			q.mu.Lock()

			for len(q.workerPool) == 0 {
				q.mu.Unlock()
				time.Sleep(1 * time.Millisecond)
				q.mu.Lock()
			}

			if len(q.workerPool) == 0 {
				panic("worker pool is empty")
			}

			workerCh := q.workerPool[0]
			q.workerPool[0] = nil
			q.workerPool = q.workerPool[1:]
			workerCh <- msg
			q.mu.Unlock()
		}
	}()
}

func (q *workerQueue) spawnWorker(w worker, ch chan getMsg) {
	for msg := range ch {
		value, err := w(msg.key)
		msg.resCh <- getResult{value, err}
		q.mu.Lock()
		q.workerPool = append(q.workerPool, ch)
		q.mu.Unlock()
	}
}

func (q *workerQueue) Do(key string) (string, error) {
	if q.isClosed {
		return "", ErrWorkerQueueIsClosed
	}
	resCh := make(chan getResult)
	q.msgQueue <- getMsg{key, resCh}
	res := <-resCh
	return res.value, res.err
}

func (q *workerQueue) Close() {
	if q.isClosed {
		return
	}
	q.isClosed = true
	q.mu.Lock()
	defer q.mu.Unlock()
	close(q.msgQueue)
	for _, ch := range q.workerPool {
		close(ch)
	}
}
