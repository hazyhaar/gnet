// Copyright (c) 2021 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package netpoll

import (
	"os"
	"sync"

	"golang.org/x/sys/unix"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/queue"
)

// PollEventHandler is the callback for I/O events notified by the poller.
type PollEventHandler func(int, IOEvent, IOFlags) error

// PollAttachment is the user data which is about to be stored in "void *ptr" of epoll_data or "void *udata" of kevent.
type PollAttachment struct {
	FD       int
	Callback PollEventHandler
}

// pollerLifecycle serializes admission, retirement and physical close.
// Polling is the only consumer while running; Close cannot drain its queues.
type pollerLifecycle struct {
	retireMu sync.Mutex
	polling  bool
	closed   bool
	closeErr error
	done     chan struct{}
}

// closedDone is returned by Done for a Poller that was never opened.
var closedDone = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Done closes once no queued task can execute, before the reactor cleans up
// its registered connections. Close also closes it if Polling never started.
// A zero-value Poller reports an already closed channel. The done field is
// written only by OpenPoller, so reading it here needs no lock.
func (p *Poller) Done() <-chan struct{} {
	if p.done == nil {
		return closedDone
	}
	return p.done
}

// isRetiredLocked reports whether the Poller is closed, uninitialized or
// retired. It requires retireMu held.
func (p *Poller) isRetiredLocked() bool {
	if p.closed || p.done == nil {
		return true
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *Poller) beginPolling() error {
	p.retireMu.Lock()
	defer p.retireMu.Unlock()
	if p.isRetiredLocked() {
		return errorx.ErrEngineShutdown
	}
	if p.polling {
		return os.NewSyscallError("polling", unix.EBUSY)
	}
	p.polling = true
	return nil
}

// enqueue requires retireMu held through the subsequent wakeup. Serializing
// producers also prevents a second Trigger from reporting success against a
// wakeupCall whose first wakeup is still in progress and may fail.
func (p *Poller) enqueue(priority queue.EventPriority, fn queue.Func, param any) error {
	if p.isRetiredLocked() {
		return errorx.ErrEngineShutdown
	}
	task := queue.GetTask()
	task.Exec, task.Param = fn, param
	if priority > queue.HighPriority && p.urgentAsyncTaskQueue.Length() >= p.highPriorityEventsThreshold {
		p.asyncTaskQueue.Enqueue(task)
	} else {
		p.urgentAsyncTaskQueue.Enqueue(task)
	}
	return nil
}

func (p *Poller) retire() {
	p.retireMu.Lock()
	defer p.retireMu.Unlock()
	p.polling = false
	p.retireLocked()
}

func (p *Poller) retireLocked() {
	if p.isRetiredLocked() {
		return
	}
	p.dropQueue(p.urgentAsyncTaskQueue)
	p.dropQueue(p.asyncTaskQueue)
	close(p.done)
}

// closePoller is idempotent. An active Polling must first be stopped through
// its callback/Trigger and joined by its owner; returning EBUSY avoids both
// concurrent drainage and deadlock if Close is called from that callback.
func (p *Poller) closePoller(closeFDs func() error) error {
	p.retireMu.Lock()
	defer p.retireMu.Unlock()
	if p.polling {
		return os.NewSyscallError("close", unix.EBUSY)
	}
	if p.done == nil {
		// Zero-value Poller: its descriptors are 0, which must not be closed.
		p.closed = true
		return nil
	}
	if !p.closed {
		p.retireLocked()
		p.closed = true
		p.closeErr = closeFDs()
	}
	return p.closeErr
}

func (p *Poller) dropQueue(q queue.AsyncTaskQueue) {
	if q == nil {
		return
	}
	for task := q.Dequeue(); task != nil; task = q.Dequeue() {
		queue.PutTask(task)
	}
}
