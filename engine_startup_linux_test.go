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

package gnet

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/panjf2000/gnet/v2/pkg/logging"
	"github.com/panjf2000/gnet/v2/pkg/netpoll"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const startupCleanupLoops = 64

// Each case runs in a fresh bounded child. This file uses only Go <= 1.20 APIs
// and is shared without adapters by default and poll_opt builds.
func TestEngineStartupCleanup(t *testing.T) {
	if mode := os.Getenv("GNET_STARTUP_CLEANUP_CHILD"); mode != "" {
		startupCleanupBoundChild(t)
		switch mode {
		case "IngressOpenPollerFailure":
			startupCleanupIngress(t)
		case "EventLoopAddReadFailure":
			startupCleanupAddRead(t)
		case "UnstartedPollerControl":
			startupCleanupControl(t)
		default:
			t.Fatalf("unknown child case %q", mode)
		}
		return
	}
	var fdBefore, vmBefore unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &fdBefore); err != nil {
		t.Fatal(err)
	}
	if err := unix.Getrlimit(unix.RLIMIT_AS, &vmBefore); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"IngressOpenPollerFailure", "EventLoopAddReadFailure", "UnstartedPollerControl"} {
		t.Run(mode, func(t *testing.T) {
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestEngineStartupCleanup$", "-test.v", "-test.timeout=6s")
			cmd.Env = append(os.Environ(), "GNET_STARTUP_CLEANUP_CHILD="+mode)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
			cmd.WaitDelay = time.Second
			output, runErr := cmd.CombinedOutput()
			t.Logf("case=%s child_error=%v\n%s", mode, runErr, output)
			if runErr != nil {
				t.Errorf("startup cleanup child failed: %v", runErr)
			}
		})
	}
	var fdAfter, vmAfter unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &fdAfter); err != nil {
		t.Fatal(err)
	}
	if err := unix.Getrlimit(unix.RLIMIT_AS, &vmAfter); err != nil {
		t.Fatal(err)
	}
	if fdAfter != fdBefore || vmAfter != vmBefore {
		t.Fatalf("parent limits changed: NOFILE %v -> %v; AS %v -> %v", fdBefore, fdAfter, vmBefore, vmAfter)
	}
	t.Logf("parent limits unchanged: NOFILE=%v AS=%v", fdAfter, vmAfter)
}

func startupCleanupBoundChild(t *testing.T) {
	t.Helper()
	runtime.GOMAXPROCS(2)
	debug.SetMaxThreads(64)
	debug.SetMemoryLimit(64 << 20)
	var vm unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_AS, &vm); err != nil {
		t.Fatal(err)
	}
	const maxVM = uint64(4 << 30)
	if vm.Cur > maxVM {
		vm.Cur = maxVM
	}
	if vm.Max > maxVM {
		vm.Max = maxVM
	}
	if err := unix.Setrlimit(unix.RLIMIT_AS, &vm); err != nil {
		t.Fatal(err)
	}
	t.Logf("child pid=%d bounds: GOMAXPROCS=2 max_threads=64 Go_memory_target=67108864 AS=%v", os.Getpid(), vm)
}

type startupCleanupBoot struct {
	BuiltinEventEngine
	eng     *engine
	t       *testing.T
	restore func()
}

func (h *startupCleanupBoot) OnBoot(e Engine) Action {
	h.eng = e.eng
	h.restore = startupCleanupExhaust(h.t, 2*startupCleanupLoops)
	return None
}

// Only the disposable child reaches Setrlimit. Exactly 2*N real fd slots are
// made available: all sub-pollers fit, but ingress epoll_create1 cannot fit.
func startupCleanupExhaust(t *testing.T, slots int) func() {
	t.Helper()
	var old unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &old); err != nil {
		t.Fatal(err)
	}
	before := startupCleanupFDs(t)
	maxFD := 2
	for fd := range before {
		if fd > maxFD {
			maxFD = fd
		}
	}
	limited := old
	limited.Cur = uint64(maxFD + slots + 32)
	if limited.Cur >= old.Cur {
		t.Fatal("setup: initial NOFILE limit is too small")
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limited); err != nil {
		t.Fatal(err)
	}
	held := make([]int, 0, slots+64)
	for {
		fd, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if errors.Is(err, unix.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, fd)
	}
	if len(held) < slots {
		t.Fatal("setup: insufficient descriptor reservations")
	}
	for _, fd := range held[len(held)-slots:] {
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
	}
	held = held[:len(held)-slots]
	t.Logf("real exhaustion: NOFILE=%v released_slots=%d", limited, slots)
	return func() {
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &old); err != nil {
			t.Fatal(err)
		}
		for _, fd := range held {
			if err := unix.Close(fd); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func startupCleanupFDs(t *testing.T) map[int]string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[int]string, len(entries))
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue
		} // ReadDir's own fd is gone.
		if err != nil {
			t.Fatal(err)
		}
		result[fd] = target
	}
	return result
}

func startupCleanupStack(t *testing.T) string {
	t.Helper()
	for size := 65536; size <= 8<<20; size *= 2 {
		buf := make([]byte, size)
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
	}
	t.Fatal("complete goroutine stack capture exceeded 8 MiB")
	return ""
}

func startupCleanupAssertClean(t *testing.T, before map[int]string, eng *engine) {
	t.Helper()
	after := startupCleanupFDs(t)
	extra, pollFDs := 0, 0
	for fd, target := range after {
		if old, exists := before[fd]; exists && old == target {
			continue
		}
		extra++
		if target == "anon_inode:[eventpoll]" || target == "anon_inode:[eventfd]" {
			pollFDs++
		}
		t.Logf("unexpected descriptor fd=%d target=%s", fd, target)
	}
	doneOpen, known := 0, 0
	inspect := func(el *eventloop) {
		known++
		select {
		case <-el.poller.Done():
		default:
			doneOpen++
		}
	}
	eng.eventLoops.iterate(func(_ int, el *eventloop) bool { inspect(el); return true })
	if eng.ingress != nil {
		inspect(eng.ingress)
	}
	stack := startupCleanupStack(t)
	orbit := strings.Count(stack, "gnet/v2.(*eventloop).orbit(")
	running := strings.Count(stack, "gnet/v2.(*eventloop).run(")
	rotate := strings.Count(stack, "gnet/v2.(*eventloop).rotate(")
	t.Logf("AFTER_RETURN extra_fds=%d poller_fds=%d orbit=%d run=%d rotate=%d done_open=%d known_pollers=%d fd_before=%d fd_after=%d", extra, pollFDs, orbit, running, rotate, doneOpen, known, len(before), len(after))
	if extra != 0 || orbit != 0 || running != 0 || rotate != 0 || doneOpen != 0 {
		t.Logf("FULL_GOROUTINE_STACKS\n%s", stack)
		t.Errorf("preparation cleanup leaked: extra_fds=%d poller_fds=%d orbit=%d run=%d rotate=%d done_open=%d; want all zero", extra, pollFDs, orbit, running, rotate, doneOpen)
	}
}

func startupCleanupIngress(t *testing.T) {
	before := startupCleanupFDs(t)
	h := &startupCleanupBoot{t: t}
	opts := &Options{NumEventLoop: startupCleanupLoops, ReadBufferCap: 1024, Logger: logging.GetDefaultLogger()}
	err := run(h, nil, opts, nil)
	if h.restore == nil || h.eng == nil {
		t.Fatal("setup: run did not call OnBoot")
	}
	h.restore()
	if !errors.Is(err, unix.EMFILE) {
		t.Fatalf("run error=%v; want actual EMFILE from ingress preparation", err)
	}
	var call *os.SyscallError
	if !errors.As(err, &call) || call.Syscall != "epoll_create1" {
		t.Fatalf("run error=%v; want epoll_create1 EMFILE", err)
	}
	created := 0
	h.eng.eventLoops.iterate(func(_ int, _ *eventloop) bool { created++; return true })
	if created != startupCleanupLoops {
		t.Fatalf("wrong failure stage: created=%d want=%d", created, startupCleanupLoops)
	}
	if h.eng.ingress != nil {
		t.Fatal("wrong failure stage: ingress unexpectedly initialized")
	}
	t.Logf("EXPECTED_PREPARATION_ERROR syscall=%s errno=EMFILE created=%d", call.Syscall, created)
	// Do not repair, wake or join anything here: these are run's postconditions.
	startupCleanupAssertClean(t, before, h.eng)
}

func startupCleanupEngine() (*engine, context.CancelFunc) {
	root, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(root)
	eng := &engine{listeners: make(map[int]*listener), opts: &Options{ReadBufferCap: 1024, Logger: logging.GetDefaultLogger()}, eventLoops: new(roundRobinLoadBalancer), eventHandler: &BuiltinEventEngine{}, turnOff: cancel}
	eng.concurrency.Group, eng.concurrency.ctx = group, ctx
	return eng, cancel
}

func startupCleanupAddRead(t *testing.T) {
	before := startupCleanupFDs(t)
	file, err := os.CreateTemp("", "gnet-startup-nonpollable-*")
	if err != nil {
		t.Fatal(err)
	}
	name := file.Name()
	t.Cleanup(func() {
		if err := os.Remove(name); err != nil {
			t.Error(err)
		}
	})
	// Transfer an independent raw descriptor to the listener, with no os.File
	// finalizer or double-close against the production listener ownership.
	fd, dupErr := unix.Dup(int(file.Fd()))
	closeErr := file.Close()
	if dupErr != nil {
		t.Fatal(dupErr)
	}
	if closeErr != nil {
		if err := unix.Close(fd); err != nil {
			t.Error(err)
		}
		t.Fatal(closeErr)
	}
	ln := &listener{fd: fd, network: "tcp"}
	eng, cancel := startupCleanupEngine()
	defer cancel()
	eng.listeners[fd] = ln
	err = eng.runEventLoops(eng.concurrency.ctx, 1)
	// Always exercise the same engine cleanup as run's error caller.
	eng.closeEventLoops()
	// Release the fixture even if the buggy loop was never registered. This
	// cannot close the leaked poller's own epoll/eventfd descriptors.
	ln.close()
	if !errors.Is(err, unix.EPERM) {
		t.Fatalf("runEventLoops error=%v; want actual EPERM for ordinary file", err)
	}
	var call *os.SyscallError
	if !errors.As(err, &call) || call.Syscall != "epoll_ctl add" {
		t.Fatalf("runEventLoops error=%v; want epoll_ctl add EPERM", err)
	}
	t.Logf("EXPECTED_PREPARATION_ERROR syscall=%s errno=EPERM", call.Syscall)
	startupCleanupAssertClean(t, before, eng)
}

func startupCleanupControl(t *testing.T) {
	before := startupCleanupFDs(t)
	eng, cancel := startupCleanupEngine()
	defer cancel()
	poller, err := netpoll.OpenPoller()
	if err != nil {
		t.Fatal(err)
	}
	eng.eventLoops.register(&eventloop{engine: eng, poller: poller})
	eng.closeEventLoops()
	startupCleanupAssertClean(t, before, eng)
}
