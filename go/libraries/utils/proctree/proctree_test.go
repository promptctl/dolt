// Copyright 2026 Dolthub, Inc.
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

// NOTICE (Apache License 2.0, section 4(b)): this file is not part of upstream
// github.com/dolthub/dolt; it was added in 2026 by Brandon Fryslie for the
// links-issue-tracker (`lit`) project. What it adds, why, and what would let
// it be dropped are recorded in the patch ledger that README.lit-fork.md at
// the root of this fork points to.

package proctree

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"testing"
	"time"
)

// The test binary re-runs itself as the processes of a three-deep tree: root
// starts child, child starts grandchild, and grandchild dials the test's
// listener, sends its pid, and blocks. Each inherits the root's stdout, so the
// grandchild holds the pipe Wait reads, as ssh and git-remote-http hold git's.
const roleEnv = "PROCTREE_TEST_ROLE"
const addrEnv = "PROCTREE_TEST_ADDR"

func TestMain(m *testing.M) {
	switch os.Getenv(roleEnv) {
	case "":
		os.Exit(m.Run())
	case "root":
		runRole("child")
	case "child":
		runRole("grandchild")
	case "stdin":
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	case "grandchild":
		conn, err := net.Dial("tcp", os.Getenv(addrEnv))
		if err != nil {
			os.Exit(3)
		}
		fmt.Fprintf(conn, "%d\n", os.Getpid())
		_, _ = io.Copy(io.Discard, conn)
		os.Exit(0)
	}
}

func runRole(next string) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), roleEnv+"="+next)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(4)
	}
	os.Exit(0)
}

// fixture is a listener the grandchild dials and the root command that grows
// the tree.
type fixture struct {
	t     *testing.T
	ln    net.Listener
	conns chan accepted
}

type accepted struct {
	conn net.Conn
	pid  int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, ln: ln, conns: make(chan accepted, 16)}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				_ = conn.Close()
				continue
			}
			pid, _ := strconv.Atoi(line[:len(line)-1])
			f.conns <- accepted{conn: conn, pid: pid}
		}
	}()
	return f
}

func (f *fixture) command(ctx context.Context) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0])
	cmd.Env = append(os.Environ(), roleEnv+"=root", addrEnv+"="+f.ln.Addr().String())
	cmd.Stdout = io.Discard // a pipe, which every member inherits
	cmd.WaitDelay = 500 * time.Millisecond
	return cmd
}

// grandchild waits for the grandchild's connection: the whole tree is up.
func (f *fixture) grandchild() accepted {
	f.t.Helper()
	select {
	case a := <-f.conns:
		f.t.Cleanup(func() {
			_ = a.conn.Close()
			if p, err := os.FindProcess(a.pid); err == nil {
				_ = p.Kill()
			}
		})
		return a
	case <-time.After(10 * time.Second):
		f.t.Fatal("the grandchild never connected: the tree did not grow")
		return accepted{}
	}
}

// closed reports whether conn's peer closed it within d.
func closed(conn net.Conn, d time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, conn)
	var netErr net.Error
	return !(errors.As(err, &netErr) && netErr.Timeout())
}

func TestCancelEndsTheWholeTree(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := f.command(ctx)
	tree, err := Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	g := f.grandchild()

	cancel()
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return within 5s of the cancel")
	}
	if !closed(g.conn, 2*time.Second) {
		t.Fatal("the grandchild's connection is still open after the cancel: the tree outlived it")
	}
	kill := tree.Settle()
	if kill.Processes != 3 || kill.Survivors == nil || *kill.Survivors != 0 || kill.Err != nil {
		t.Fatalf("kill = %+v, want 3 processes, 0 survivors, no error", kill)
	}
}

// The fixture engages: under exec's own cancel, which kills only the root,
// the grandchild keeps its connection. Without this, the test above could
// pass on a fixture whose grandchild died with its parent.
func TestDefaultCancelLeavesTheGrandchildConnected(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := f.command(ctx)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	g := f.grandchild()
	cancel()
	_ = cmd.Wait()
	if closed(g.conn, time.Second) {
		t.Fatal("the grandchild's connection closed under exec's default cancel: the fixture does not hold the pipe the way a transport does")
	}
}

func TestNoCancelNoKill(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^$")
	tree, err := Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if kill := tree.Settle(); kill != (Kill{}) {
		t.Fatalf("Settle reports a kill (%+v) for a command that exited on its own", kill)
	}
}

func TestStartRefusesACommandWithoutContext(t *testing.T) {
	if _, err := Start(exec.Command(os.Args[0])); err == nil {
		t.Fatal("Start accepted a command with no context")
	}
}

func TestAlreadyCancelledStartsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	if _, err := Start(cmd); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start under a cancelled context = %v, want context.Canceled", err)
	}
	if cmd.Process != nil {
		t.Fatal("a process was started under a cancelled context")
	}
}

// A cancel racing the start, at every point in it, still ends the tree.
func TestCancelRacingStartEndsTheTree(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := f.command(ctx)
		go func() {
			time.Sleep(time.Duration(i%10) * time.Millisecond)
			cancel()
		}()
		if _, err := Start(cmd); err != nil {
			cancel()
			continue // cancelled before the process existed
		}
		waited := make(chan struct{})
		go func() { _ = cmd.Wait(); close(waited) }()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d: Wait did not return within 5s of the cancel", i)
		}
		cancel()
	}
	// Every grandchild that got as far as connecting was ended with its tree.
	for {
		select {
		case a := <-f.conns:
			if !closed(a.conn, 2*time.Second) {
				t.Fatalf("grandchild %d still connected after its tree was cancelled", a.pid)
			}
		default:
			return
		}
	}
}

func TestNewMembers(t *testing.T) {
	table := []row{
		{pid: 10, ppid: 1},  // the root
		{pid: 11, ppid: 10}, // its child
		{pid: 12, ppid: 11}, // its grandchild, one ring further
		{pid: 13, ppid: 10}, // a second child
		{pid: 20, ppid: 1},  // unrelated
		{pid: 21, ppid: 20}, // unrelated child
		{pid: 30, ppid: 1},  // an orphan: reparented away from the tree
	}
	pids := func(rows []row) []int {
		var out []int
		for _, r := range rows {
			out = append(out, r.pid)
		}
		slices.Sort(out)
		return out
	}
	members := map[int]bool{10: true}
	ring := pids(newMembers(table, members))
	if !slices.Equal(ring, []int{11, 13}) {
		t.Fatalf("first ring = %v, want [11 13]", ring)
	}
	for _, pid := range ring {
		members[pid] = true
	}
	if ring := pids(newMembers(table, members)); !slices.Equal(ring, []int{12}) {
		t.Fatalf("second ring = %v, want [12]", ring)
	}
	members[12] = true
	if ring := newMembers(table, members); len(ring) != 0 {
		t.Fatalf("closed tree grew: %v", ring)
	}
}

func TestStillRunning(t *testing.T) {
	killed := []proc{{pid: 11, start: 100}, {pid: 12, start: 101}, {pid: 13, start: 102}, {pid: 14, start: 103}}
	table := []row{
		{pid: 11, start: 100},               // still running: a survivor
		{pid: 12, start: 101, zombie: true}, // exited, not yet reaped
		{pid: 13, start: 999},               // 13 was reaped and its pid reused
		{pid: 50, start: 100},               // unrelated
		// 14 is absent: exited and reaped
	}
	if n := stillRunning(table, killed); n != 1 {
		t.Fatalf("stillRunning = %d, want 1 (only pid 11)", n)
	}
}

// Settle's readings wait on the killed processes' exit, not on how soon
// after the kill they are taken.
func TestAwaitExitWaitsForTheLastExit(t *testing.T) {
	readings := []int{3, 2, 2, 1, 0}
	reads := 0
	running := func() (int, error) {
		n := readings[reads]
		reads++
		return n, nil
	}
	if n, err := awaitExit(running, time.Minute); n == nil || *n != 0 || err != nil {
		t.Fatalf("awaitExit = %v, %v; want 0, nil", n, err)
	}
	if reads != len(readings) {
		t.Fatalf("read %d times, want %d: it stopped before the last exit", reads, len(readings))
	}
}

func TestAwaitExitReportsWhatOutlivesTheBound(t *testing.T) {
	start := time.Now()
	n, err := awaitExit(func() (int, error) { return 1, nil }, 50*time.Millisecond)
	if n == nil || *n != 1 || err != nil {
		t.Fatalf("awaitExit = %v, %v; want 1, nil", n, err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond || elapsed > time.Second {
		t.Fatalf("awaitExit returned after %s, want the 50ms bound", elapsed)
	}
}

func TestAwaitExitStopsOnATableError(t *testing.T) {
	boom := errors.New("boom")
	// A failed reading is no count at all, not a count of zero.
	if n, err := awaitExit(func() (int, error) { return 0, boom }, time.Minute); n != nil || !errors.Is(err, boom) {
		t.Fatalf("awaitExit = %v, %v; want no count and boom", n, err)
	}
}
