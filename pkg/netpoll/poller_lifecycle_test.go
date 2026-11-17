// Copyright (c) 2026 The Gnet Authors. All rights reserved.
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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/queue"
)

func openLifecyclePoller(t *testing.T) (*Poller, func()) {
	t.Helper()
	p, err := OpenPoller()
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	closeOnce := func() {
		t.Helper()
		if !closed {
			closed = true
			if err := p.Close(); err != nil {
				t.Errorf("first Close = %v, want nil", err)
			}
		}
	}
	t.Cleanup(closeOnce)
	return p, closeOnce
}

func assertPollerQueuesEmpty(t *testing.T, p *Poller) {
	t.Helper()
	for _, pending := range []queue.AsyncTaskQueue{p.urgentAsyncTaskQueue, p.asyncTaskQueue} {
		count := 0
		// No Polling or producers exist: Dequeue is an exact terminal oracle,
		// unlike a transient Length observation. Also release baseline leftovers.
		for task := pending.Dequeue(); task != nil; task = pending.Dequeue() {
			count++
			queue.PutTask(task)
		}
		if count != 0 {
			t.Errorf("closed poller retained %d tasks in a queue, want 0", count)
		}
	}
}

func checkPollerRetires(t *testing.T, start func(*Poller) error) {
	t.Helper()
	p, _ := openLifecyclePoller(t)
	var behind atomic.Bool
	if err := p.Trigger(queue.HighPriority, func(any) error { return errorx.ErrEngineShutdown }, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Trigger(queue.LowPriority, func(any) error {
		behind.Store(true)
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- start(p) }()
	select {
	case err := <-errCh:
		if !errors.Is(err, errorx.ErrEngineShutdown) {
			t.Fatalf("Polling returned %v, want ErrEngineShutdown", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Polling still blocked 3s after shutdown task")
	}
	if behind.Load() {
		t.Fatal("task queued behind shutdown was executed")
	}
	if !p.urgentAsyncTaskQueue.IsEmpty() || !p.asyncTaskQueue.IsEmpty() {
		t.Fatal("retired poller left tasks queued")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done was not closed when Polling returned")
	}
	var after atomic.Bool
	err := p.Trigger(queue.HighPriority, func(any) error {
		after.Store(true)
		return nil
	}, nil)
	if !errors.Is(err, errorx.ErrEngineShutdown) {
		t.Fatalf("Trigger after retire returned %v, want ErrEngineShutdown", err)
	}
	if after.Load() {
		t.Fatal("Trigger after retire executed the task")
	}
}

// checkTriggerRacesRetire runs producers that keep calling Trigger while
// Polling retires. Every Trigger that returned nil must have left its task
// where retire could see it, so the queues are empty once all producers stop.
func checkTriggerRacesRetire(t *testing.T, start func(*Poller) error) {
	t.Helper()
	const rounds, producers = 200, 16
	for round := 0; round < rounds; round++ {
		p, closePoller := openLifecyclePoller(t)
		var accepted atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < producers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for p.Trigger(queue.LowPriority, func(any) error { return nil }, nil) == nil {
					accepted.Add(1)
				}
			}()
		}
		errCh := make(chan error, 1)
		go func() { errCh <- start(p) }()
		for accepted.Load() < 256 {
			runtime.Gosched()
		}
		if err := p.Trigger(queue.HighPriority, func(any) error { return errorx.ErrEngineShutdown }, nil); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, errorx.ErrEngineShutdown) {
				t.Fatalf("Polling returned %v, want ErrEngineShutdown", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Polling still blocked 3s after shutdown task")
		}
		wg.Wait()
		if !p.urgentAsyncTaskQueue.IsEmpty() || !p.asyncTaskQueue.IsEmpty() {
			t.Fatalf("round %d: Trigger enqueued after the final drain", round)
		}
		closePoller()
	}
}

func TestPollerCloseBeforePollingClosesDone(t *testing.T) {
	p, closePoller := openLifecyclePoller(t)
	select {
	case <-p.Done():
		t.Fatal("new poller already has closed Done")
	default:
	}
	closePoller()
	select {
	case <-p.Done():
	default:
		t.Fatal("Close returned before Polling ever started but Done remains open")
	}
}

func TestPollerTriggerAfterCloseRejects(t *testing.T) {
	p, closePoller := openLifecyclePoller(t)
	closePoller()
	var ran atomic.Bool
	for _, priority := range []queue.EventPriority{queue.HighPriority, queue.LowPriority} {
		err := p.Trigger(priority, func(any) error { ran.Store(true); return nil }, nil)
		if !errors.Is(err, errorx.ErrEngineShutdown) {
			t.Errorf("Trigger(%v) after Close = %v, want ErrEngineShutdown", priority, err)
		}
	}
	if ran.Load() {
		t.Error("rejected callback executed")
	}
	assertPollerQueuesEmpty(t, p)
}

func TestPollerCloseBeforePollingDropsTasks(t *testing.T) {
	p, closePoller := openLifecyclePoller(t)
	var ran atomic.Bool
	for _, priority := range []queue.EventPriority{queue.HighPriority, queue.LowPriority} {
		if err := p.Trigger(priority, func(any) error { ran.Store(true); return nil }, nil); err != nil {
			t.Fatalf("Trigger before Close = %v, want nil", err)
		}
	}
	closePoller()
	if ran.Load() {
		t.Error("Close executed a callback without Polling")
	}
	assertPollerQueuesEmpty(t, p)
}

func checkZeroValuePollerLifecycle(t *testing.T, start func(*Poller) error) {
	t.Helper()
	t.Run("Done", func(t *testing.T) {
		p := &Poller{}
		select {
		case <-p.Done():
		default:
			t.Error("zero-value Done blocks, want an already closed channel")
		}
	})
	for _, priority := range []queue.EventPriority{queue.HighPriority, queue.LowPriority} {
		name := "TriggerHighPriority"
		if priority == queue.LowPriority {
			name = "TriggerLowPriority"
		}
		t.Run(name, func(t *testing.T) {
			p := &Poller{}
			var ran atomic.Bool
			defer func() {
				if failure := recover(); failure != nil {
					t.Errorf("zero-value Trigger panicked: %v", failure)
				}
				if ran.Load() {
					t.Error("zero-value Trigger executed the callback")
				}
			}()
			if err := p.Trigger(priority, func(any) error { ran.Store(true); return nil }, nil); !errors.Is(err, errorx.ErrEngineShutdown) {
				t.Errorf("zero-value Trigger(%v) = %v, want ErrEngineShutdown", priority, err)
			}
		})
	}
	t.Run("Polling", func(t *testing.T) {
		p := &Poller{}
		defer func() {
			if failure := recover(); failure != nil {
				t.Errorf("zero-value Polling panicked: %v", failure)
			}
		}()
		if err := start(p); !errors.Is(err, errorx.ErrEngineShutdown) {
			t.Errorf("zero-value Polling = %v, want ErrEngineShutdown", err)
		}
	})
	t.Run("Close", func(t *testing.T) {
		p := &Poller{}
		if _, err := unix.FcntlInt(0, unix.F_GETFD, 0); err != nil {
			t.Fatalf("stdin before Close = %v, want an open descriptor", err)
		}
		for attempt := 1; attempt <= 2; attempt++ {
			func() {
				defer func() {
					if failure := recover(); failure != nil {
						t.Errorf("zero-value Close attempt %d panicked: %v", attempt, failure)
					}
				}()
				if err := p.Close(); err != nil {
					t.Errorf("zero-value Close attempt %d = %v, want nil", attempt, err)
				}
			}()
			if _, err := unix.FcntlInt(0, unix.F_GETFD, 0); err != nil {
				t.Errorf("stdin after Close attempt %d = %v, want an open descriptor", attempt, err)
			}
			select {
			case <-p.Done():
			default:
				t.Errorf("zero-value Done blocks after Close attempt %d", attempt)
			}
		}
	})
}
