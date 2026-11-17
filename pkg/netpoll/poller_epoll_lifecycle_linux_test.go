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

//go:build linux

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

func awaitPollerResult(t *testing.T, result <-chan error) error {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal("poller operation did not finish")
		return nil
	}
}

func assertFDAlive(t *testing.T, fd int) {
	t.Helper()
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
		t.Fatalf("live descriptor %d: %v", fd, err)
	}
}

func TestPollerCloseActivePreservesTasksAndDescriptors(t *testing.T) {
	p, closePoller := openLifecyclePoller(t)
	epollFD, wakeFD := p.fd, pollerWakeFD(p)
	entered := make(chan error, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	if err := p.Trigger(queue.HighPriority, func(any) error {
		// Close from the callback must also be non-blocking and return EBUSY.
		entered <- p.Close()
		<-release
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	joined := make(chan error, 1)
	go func() { joined <- runPollerForTest(p) }()
	joinedAlready := false
	t.Cleanup(func() {
		unblock()
		if !joinedAlready {
			if err := p.Trigger(queue.HighPriority, func(any) error { return errorx.ErrEngineShutdown }, nil); err != nil {
				t.Error(err)
			}
			if err := awaitPollerResult(t, joined); !errors.Is(err, errorx.ErrEngineShutdown) {
				t.Error(err)
			}
		}
	})
	if err := awaitPollerResult(t, entered); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("Close in active callback = %v, want EBUSY", err)
	}
	var retained atomic.Bool
	if err := p.Trigger(queue.LowPriority, func(any) error { retained.Store(true); return nil }, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("Close while callback held = %v, want EBUSY", err)
	}
	// A second Polling must not retire the first poller's queues on rejection.
	second := make(chan error, 1)
	go func() { second <- runPollerForTest(p) }()
	if err := awaitPollerResult(t, second); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("second Polling = %v, want EBUSY", err)
	}
	select {
	case <-p.Done():
		t.Fatal("busy Close or rejected Polling closed Done")
	default:
	}
	assertFDAlive(t, epollFD)
	assertFDAlive(t, wakeFD)
	if err := p.Trigger(queue.HighPriority, func(any) error { return errorx.ErrEngineShutdown }, nil); err != nil {
		t.Fatal(err)
	}
	unblock()
	err := awaitPollerResult(t, joined)
	joinedAlready = true
	if !errors.Is(err, errorx.ErrEngineShutdown) {
		t.Fatalf("Polling after release = %v", err)
	}
	if !retained.Load() {
		t.Fatal("busy Close drained or lost an admitted callback")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("joined Polling left Done open")
	}
	// Retirement is deliberately not physical close.
	assertFDAlive(t, epollFD)
	assertFDAlive(t, wakeFD)
	closePoller()
	for _, fd := range []int{epollFD, wakeFD} {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Errorf("descriptor %d after joined Close = %v, want EBADF", fd, err)
		}
	}
}

type gatedQueue struct {
	queue.AsyncTaskQueue
	entered chan struct{}
	release <-chan struct{}
}

func (q *gatedQueue) Enqueue(task *queue.Task) {
	q.AsyncTaskQueue.Enqueue(task)
	close(q.entered)
	<-q.release
}

func TestPollerTriggerGateOrdersWakeBeforeClose(t *testing.T) {
	p, closePoller := openLifecyclePoller(t)
	epollFD, wakeFD := p.fd, pollerWakeFD(p)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	gate := &gatedQueue{AsyncTaskQueue: p.urgentAsyncTaskQueue, entered: make(chan struct{}), release: release}
	p.urgentAsyncTaskQueue = gate
	triggered := make(chan error, 1)
	go func() { triggered <- p.Trigger(queue.HighPriority, func(any) error { return nil }, nil) }()
	t.Cleanup(unblock)
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Trigger did not reach gated insertion")
	}
	// The queue barrier is an actual point within Trigger, without a global
	// hook. TryLock gives a synchronous negative control for the shared gate.
	if p.retireMu.TryLock() {
		p.retireMu.Unlock()
		t.Fatal("Trigger did not retain the lifecycle gate during admission")
	}
	closeStarted := make(chan struct{})
	closed := make(chan error, 1)
	go func() { close(closeStarted); closed <- p.Close() }()
	<-closeStarted
	assertFDAlive(t, epollFD)
	assertFDAlive(t, wakeFD)
	select {
	case err := <-closed:
		t.Fatalf("Close crossed a held producer gate: %v", err)
	default:
	}
	unblock()
	if err := awaitPollerResult(t, triggered); err != nil {
		t.Fatalf("admitted Trigger wakeup = %v, want nil before physical close", err)
	}
	if err := awaitPollerResult(t, closed); err != nil {
		t.Fatal(err)
	}
	closePoller()
	assertPollerQueuesEmpty(t, p)
	if err := p.Trigger(queue.HighPriority, func(any) error { return nil }, nil); !errors.Is(err, errorx.ErrEngineShutdown) {
		t.Fatalf("Trigger after gated Close = %v, want ErrEngineShutdown", err)
	}
}

func TestPollerSaturatedWakeIsPendingEvent(t *testing.T) {
	p, closePoller := openLifecyclePoller(t)
	wakeFD := pollerWakeFD(p)
	// Read native-endian one from the kernel, independent of package b.
	seed, err := unix.Eventfd(1, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	var probe [8]byte
	n, readErr := unix.Read(seed, probe[:])
	closeErr := unix.Close(seed)
	if readErr != nil || closeErr != nil || n != 8 {
		t.Fatalf("native eventfd seed: read=(%d, %v), close=%v", n, readErr, closeErr)
	}
	if n, err := unix.Write(wakeFD, probe[:]); err != nil || n != 8 {
		t.Fatalf("eventfd write control = (%d, %v)", n, err)
	}
	var readback [8]byte
	if n, err := unix.Read(wakeFD, readback[:]); err != nil || n != 8 || readback != probe {
		t.Fatalf("eventfd roundtrip = (%d, %v, %v), want exact probe", n, err, readback)
	}
	// The following EAGAIN is the independent kernel saturation oracle.
	var full [8]byte
	for i := range full {
		full[i] = 0xff
	}
	oneIndex := 0
	if probe[0] != 1 {
		oneIndex = 7
	}
	full[oneIndex] = 0xfe // UINT64_MAX-1 is the maximum eventfd counter.
	if n, err := unix.Write(wakeFD, full[:]); err != nil || n != 8 {
		t.Fatalf("saturate eventfd = (%d, %v)", n, err)
	}
	if _, err := unix.Write(wakeFD, probe[:]); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("negative full-eventfd control = %v, want EAGAIN", err)
	}
	var ran atomic.Bool
	if err := p.Trigger(queue.HighPriority, func(any) error { ran.Store(true); return errorx.ErrEngineShutdown }, nil); err != nil {
		t.Fatalf("Trigger with pending saturated wake = %v, want nil", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- runPollerForTest(p) }()
	if err := awaitPollerResult(t, joined); !errors.Is(err, errorx.ErrEngineShutdown) {
		t.Fatalf("Polling saturated event = %v", err)
	}
	if !ran.Load() {
		t.Fatal("pending wake did not run accepted callback")
	}
	closePoller()
}

func nativeEventfdOne(t *testing.T) [8]byte {
	t.Helper()
	fd, err := unix.Eventfd(1, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	var one [8]byte
	n, readErr := unix.Read(fd, one[:])
	closeErr := unix.Close(fd)
	if n != 8 || readErr != nil || closeErr != nil {
		t.Fatalf("eventfd seed: read=(%d, %v), close=%v", n, readErr, closeErr)
	}
	return one
}

// startWakePolling always joins Polling, including when the test proves a lost
// edge. Recovery is deliberately outside the progress assertion: it drains the
// counter and writes a fresh kernel edge, after queuing a shutdown callback.
func startWakePolling(t *testing.T, p *Poller, one [8]byte) <-chan struct{} {
	t.Helper()
	finished := make(chan struct{})
	var pollErr error
	go func() {
		pollErr = runPollerForTest(p)
		close(finished)
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
		default:
			if err := p.Trigger(queue.HighPriority, func(any) error { return errorx.ErrEngineShutdown }, nil); err != nil && !errors.Is(err, errorx.ErrEngineShutdown) {
				t.Errorf("cleanup shutdown submission: %v", err)
			}
			var drained [8]byte
			fd := pollerWakeFD(p)
			for {
				_, err := unix.Read(fd, drained[:])
				if errors.Is(err, unix.EINTR) {
					continue
				}
				if err != nil && !errors.Is(err, unix.EAGAIN) {
					t.Errorf("cleanup drain: %v", err)
				}
				break
			}
			for {
				_, err := unix.Write(fd, one[:])
				if errors.Is(err, unix.EINTR) {
					continue
				}
				if err != nil {
					t.Errorf("cleanup manual wake: %v", err)
				}
				break
			}
			select {
			case <-finished:
				t.Log("cleanup joined Polling after an explicit eventfd drain and fresh wake")
			case <-time.After(3 * time.Second):
				t.Error("cleanup could not join Polling after manual wake")
				return
			}
		}
		if !errors.Is(pollErr, errorx.ErrEngineShutdown) {
			t.Errorf("joined Polling = %v, want ErrEngineShutdown", pollErr)
		}
	})
	return finished
}

func TestPollerWakeSaturationAfterConsumedEdgeMakesProgress(t *testing.T) {
	p, _ := openLifecyclePoller(t)
	one := nativeEventfdOne(t)
	var saturated [8]byte
	for i := range saturated {
		saturated[i] = 0xff
	}
	if one[0] == 1 {
		saturated[0] = 0xfe
	} else {
		saturated[7] = 0xfe
	}
	wakeFD := pollerWakeFD(p)
	if n, err := unix.Write(wakeFD, saturated[:]); n != 8 || err != nil {
		t.Fatalf("eventfd saturation = (%d, %v)", n, err)
	}
	if _, err := unix.Write(wakeFD, one[:]); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("full-eventfd negative control = %v, want EAGAIN", err)
	}
	first := make(chan struct{})
	if err := p.Trigger(queue.HighPriority, func(any) error { close(first); return nil }, nil); err != nil {
		t.Fatal(err)
	}
	finished := startWakePolling(t, p, one)
	select {
	case <-first:
	case <-time.After(3 * time.Second):
		t.Fatal("first pending edge did not execute its callback")
	}
	// The callback has returned only once the loop resets its wake flag. Wait
	// for that actual boundary instead of guessing when epoll consumed the edge.
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&p.wakeupCall) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("poller did not reset wakeupCall after the first callback")
		}
		runtime.Gosched()
	}
	second := make(chan struct{})
	if err := p.Trigger(queue.HighPriority, func(any) error {
		close(second)
		return errorx.ErrEngineShutdown
	}, nil); err != nil {
		t.Fatalf("second Trigger = %v, want progress or an explicit terminal error", err)
	}
	select {
	case <-second:
	case <-time.After(3 * time.Second):
		t.Fatal("second callback stalled: Trigger returned nil after the ET edge had already been consumed")
	}
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown callback ran but Polling did not return")
	}
}

func TestPollerWakeFatalWriteResetsAdmissionForNextWake(t *testing.T) {
	p, _ := openLifecyclePoller(t)
	one := nativeEventfdOne(t)
	wakeFD := pollerWakeFD(p)
	_, controlErr := unix.Write(-1, one[:])
	if !errors.Is(controlErr, unix.EBADF) {
		t.Fatalf("negative write control = %v, want EBADF", controlErr)
	}
	order := make(chan int, 2)
	// No Polling exists yet. The injection is local to this poller and is
	// restored immediately after the real Trigger returns, even if it failed.
	setPollerWakeFD(p, -1)
	err := p.Trigger(queue.HighPriority, func(any) error { order <- 1; return nil }, nil)
	setPollerWakeFD(p, wakeFD)
	if !errors.Is(err, unix.EBADF) {
		t.Fatalf("Trigger on invalid wake descriptor = %v, want EBADF", err)
	}
	if got := atomic.LoadInt32(&p.wakeupCall); got != 0 {
		t.Fatalf("wake flag after fatal write = %d, want 0", got)
	}
	select {
	case value := <-order:
		t.Fatalf("failed wake executed callback before Polling: %d", value)
	default:
	}
	if err := p.Trigger(queue.HighPriority, func(any) error { order <- 2; return errorx.ErrEngineShutdown }, nil); err != nil {
		t.Fatalf("Trigger after restoring wake descriptor = %v", err)
	}
	finished := startWakePolling(t, p, one)
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("successful retry wake did not progress after the fatal write")
	}
	for _, want := range []int{1, 2} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("callback order = %d, want %d", got, want)
			}
		default:
			t.Fatalf("missing callback %d after joined Polling", want)
		}
	}
}

func TestPollerRepeatedClosePreservesReusedDescriptor(t *testing.T) {
	// F_DUPFD_CLOEXEC allocates, never overwrites an unrelated descriptor.
	// Keep the source open before creating the poller so it cannot occupy fd.
	source, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Close(source); err != nil {
			t.Error(err)
		}
	})
	p, closePoller := openLifecyclePoller(t)
	originalFD := p.fd
	closePoller()
	reused, err := unix.FcntlInt(uintptr(source), unix.F_DUPFD_CLOEXEC, originalFD)
	if err != nil {
		t.Fatal(err)
	}
	alive := true
	t.Cleanup(func() {
		if alive {
			if err := unix.Close(reused); err != nil {
				t.Error(err)
			}
		}
	})
	if reused != originalFD {
		t.Fatalf("kernel allocated fd %d instead of closed poller fd %d; reuse precondition not established", reused, originalFD)
	}
	if _, err := unix.FcntlInt(uintptr(reused), unix.F_GETFD, 0); err != nil {
		t.Fatalf("reuse witness is not live before repeated Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("repeated Close = %v, want nil", err)
	}
	if _, err := unix.FcntlInt(uintptr(reused), unix.F_GETFD, 0); err != nil {
		alive = !errors.Is(err, unix.EBADF)
		t.Fatalf("repeated Close destroyed reused descriptor %d: %v", reused, err)
	}
	if err := unix.Close(reused); err != nil {
		t.Fatal(err)
	}
	alive = false
	if _, err := unix.FcntlInt(uintptr(reused), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("closed witness F_GETFD = %v, want EBADF", err)
	}
}
