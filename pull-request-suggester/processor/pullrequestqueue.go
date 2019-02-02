package processor

import (
	"github.com/google/go-github/github"
	"net/http"
	"sync"
	"time"
)

type PullRequestEvent struct {
	Request *http.Request
	Event   *github.PullRequestEvent
}

var (
	queue      []*PullRequestEvent
	queuemutex = sync.Mutex{}
)

// RegisterRequest is used to drop a message in the queue
func RegisterRequest(request *http.Request, event *github.PullRequestEvent) {
	pre := &PullRequestEvent{
		Request: request,
		Event:   event,
	}
	queuemutex.Lock()
	queue = append(queue, pre)
	queuemutex.Unlock()
}

// Next will return a message from the queue FIFO
// If no message is in the queue, the function will hang
func Next() *PullRequestEvent {
	var nextEvent *PullRequestEvent
	for {
		queuemutex.Lock()
		if len(queue) == 0 {
			queuemutex.Unlock()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		nextEvent, queue = queue[len(queue)-1], queue[:len(queue)-1]
		queuemutex.Unlock()
		time.Sleep(time.Millisecond * 100)
		break
	}
	return nextEvent
}
