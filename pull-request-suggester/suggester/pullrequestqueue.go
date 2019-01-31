package suggester

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

func RegisterRequest(request *http.Request, event *github.PullRequestEvent) {
	pre := &PullRequestEvent{
		Request: request,
		Event:   event,
	}
	queuemutex.Lock()
	queue = append(queue, pre)
	queuemutex.Unlock()
}

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
