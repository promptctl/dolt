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

package gitcmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// blackHole accepts connections and never answers, as a remote that has
// stopped responding mid-transfer does.
func blackHole(t *testing.T) (addr string, accepted <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
			conns <- conn
		}
	}()
	return ln.Addr().String(), conns
}

// A cancelled git that is waiting on a remote is ended with its transport:
// the remote sees its connection close. Over http the connection belongs to
// git-remote-http, a child of git; over git:// to git itself.
func TestCancelEndsGitAndItsTransport(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	for _, tc := range []struct {
		scheme       string
		minProcesses int
	}{
		{scheme: "http", minProcesses: 2},
		{scheme: "git", minProcesses: 1},
	} {
		t.Run(tc.scheme, func(t *testing.T) {
			addr, accepted := blackHole(t)
			var observed []*Cancelled
			ctx, cancel := context.WithCancel(WithObserver(context.Background(), func(c *Cancelled) { observed = append(observed, c) }))
			defer cancel()
			cmd := Command(ctx, Spec{Dir: t.TempDir(), Env: []string{"GIT_TERMINAL_PROMPT=0", "HOME=" + t.TempDir()}},
				"ls-remote", tc.scheme+"://"+addr+"/wedge.git")
			done := make(chan error, 1)
			go func() {
				_, err := cmd.Output()
				done <- err
			}()
			var conn net.Conn
			select {
			case conn = <-accepted:
			case err := <-done:
				t.Fatalf("git returned before reaching the remote: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("git never connected to the remote")
			}

			cancel()
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("git did not return within 2s of the cancel")
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			var netErr net.Error
			if _, readErr := io.Copy(io.Discard, conn); errors.As(readErr, &netErr) && netErr.Timeout() {
				t.Fatal("the remote's connection is still open 2s after the cancel: the transport outlived git")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want it to wrap context.Canceled", err)
			}
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("err = %v, want it to wrap the *exec.ExitError of the ended git", err)
			}
			var cancelled *Cancelled
			if !errors.As(err, &cancelled) {
				t.Fatalf("err = %v, want a *Cancelled", err)
			}
			if k := cancelled.Kill; k.Processes < tc.minProcesses || k.Survivors == nil || *k.Survivors != 0 || k.Err != nil {
				t.Fatalf("kill = %+v, want at least %d process(es), no survivors, no error", k, tc.minProcesses)
			}
			if len(observed) != 1 || observed[0] != cancelled {
				t.Fatalf("observer saw %v, want exactly the returned *Cancelled", observed)
			}
		})
	}
}

func TestGitThatExitsOnItsOwnIsNotCancelled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	observed := 0
	ctx := WithObserver(context.Background(), func(*Cancelled) { observed++ })
	out, err := Command(ctx, Spec{}, "version").Output()
	if err != nil {
		t.Fatal(err)
	}
	if observed != 0 {
		t.Fatalf("the observer saw %d cancel(s) of a git that exited on its own", observed)
	}
	if !strings.HasPrefix(string(out), "git version ") {
		t.Fatalf("output = %q, want git's version", out)
	}
}

// Output keeps exec's contract: a failed git's stderr is on its ExitError.
func TestOutputPutsStderrOnTheExitError(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	_, err := Command(context.Background(), Spec{Dir: t.TempDir()}, "--git-dir", "does-not-exist", "rev-parse", "HEAD").Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("err = %v, want an *exec.ExitError", err)
	}
	if !strings.Contains(string(exitErr.Stderr), "not a git repository") {
		t.Fatalf("ExitError.Stderr = %q, want git's complaint", exitErr.Stderr)
	}
	var cancelled *Cancelled
	if errors.As(err, &cancelled) {
		t.Fatal("an uncancelled failure reads as a cancel")
	}
}

func TestStreamCloseReturnsHowGitEnded(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	var stderr strings.Builder
	r, err := Command(context.Background(), Spec{}, "version").Stream(&stderr)
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close = %v, want nil for a git that exited 0", err)
	}
	if !strings.HasPrefix(string(out), "git version ") {
		t.Fatalf("streamed %q, want git's version", out)
	}
}

// An uncancelled git's output is complete: a descendant that writes after git
// has exited is still read, however long after. A WaitDelay of a second would
// cut this output at the first write.
func TestUncancelledOutputIsReadToTheEnd(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	out, err := Command(context.Background(), Spec{},
		"-c", "alias.late=!f() { (sleep 1.5; echo late) & echo early; }; f", "late").Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(out); got != "early\nlate\n" {
		t.Fatalf("output = %q, want both writes", got)
	}
}

// After a cancel, a process outside the tree that holds git's output is not
// waited on past the bound, and the cancel says so.
func TestCancelledOutputCutOutsideTheTree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	for _, tc := range []struct {
		name string
		// alias starts a sleeper holding stdout and writes its pid to
		// $PIDFILE once it is where the case puts it, then keeps git
		// running.
		alias string
		cut   bool
	}{
		{
			name:  "detached",
			alias: `!f() { ( sleep 30 & echo $! > "$PIDFILE.tmp" ); mv "$PIDFILE.tmp" "$PIDFILE"; sleep 30; }; f`,
			cut:   true,
		},
		{
			name:  "in the tree",
			alias: `!f() { sleep 30 & echo $! > "$PIDFILE.tmp"; mv "$PIDFILE.tmp" "$PIDFILE"; sleep 30; }; f`,
			cut:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "sleeper")
			t.Cleanup(func() {
				if b, err := os.ReadFile(pidFile); err == nil {
					if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
						if p, err := os.FindProcess(pid); err == nil {
							_ = p.Kill()
						}
					}
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cmd := Command(ctx, Spec{Env: append(os.Environ(), "PIDFILE="+pidFile)}, "-c", "alias.hold="+tc.alias, "hold")
			done := make(chan error, 1)
			go func() {
				_, err := cmd.Output()
				done <- err
			}()
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(pidFile); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the sleeper never started")
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			var err error
			select {
			case err = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("the cancelled run did not return within 3s")
			}
			var cancelled *Cancelled
			if !errors.As(err, &cancelled) {
				t.Fatalf("err = %v, want a *Cancelled", err)
			}
			if cancelled.OutputCut != tc.cut {
				t.Fatalf("OutputCut = %t, want %t (kill %+v)", cancelled.OutputCut, tc.cut, cancelled.Kill)
			}
		})
	}
}

// lateHolder is a git alias whose shell backgrounds a writer and returns, so
// git exits while the writer holds stdout and stderr. The writer waits for git
// to be gone, touches $MARK, and holds the output 30s more before writing
// "late". A cancel after $MARK exists lands after git has exited.
const lateHolder = `!f() { g=$PPID; ( while kill -0 $g 2>/dev/null; do sleep 0.01; done; : > "$MARK"; sleep 30; echo late ) & echo early; }; f`

// awaitFile waits up to 10s for path to exist.
func awaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never appeared", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A context done after git has exited still bounds the run: exec has no
// process left to cancel, so nothing is killed, and the output a leftover
// process holds is cut outputBound later and recorded as cut.
func TestCancelAfterGitExitsBoundsTheOutput(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	for _, tc := range []struct {
		name string
		run  func(*Cmd) (string, error)
	}{
		{"Output", func(c *Cmd) (string, error) {
			out, err := c.Output()
			return string(out), err
		}},
		{"Stream", func(c *Cmd) (string, error) {
			r, err := c.Stream(io.Discard)
			if err != nil {
				return "", err
			}
			out, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if !errors.Is(readErr, closeErr) {
				return string(out), fmt.Errorf("read ended with %v, Close with %v: want the same error", readErr, closeErr)
			}
			return string(out), readErr
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mark := filepath.Join(t.TempDir(), "git-exited")
			var observed []*Cancelled
			ctx, cancel := context.WithCancel(WithObserver(context.Background(), func(c *Cancelled) { observed = append(observed, c) }))
			defer cancel()
			cmd := Command(ctx, Spec{Env: append(os.Environ(), "MARK="+mark)}, "-c", "alias.late="+lateHolder, "late")
			type result struct {
				out string
				err error
			}
			done := make(chan result, 1)
			go func() {
				out, err := tc.run(cmd)
				done <- result{out, err}
			}()
			awaitFile(t, mark)
			cancel()
			var r result
			select {
			case r = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("the run did not return within 3s of a cancel that came after git exited")
			}
			var cancelled *Cancelled
			if !errors.As(r.err, &cancelled) {
				t.Fatalf("err = %v, want a *Cancelled", r.err)
			}
			if cancelled.Kill.Processes != 0 || !cancelled.OutputCut || !errors.Is(r.err, context.Canceled) {
				t.Fatalf("cancelled = %+v, want no process ended, output cut, context.Canceled", cancelled)
			}
			if r.out != "early\n" {
				t.Fatalf("output = %q, want what was written before the cut", r.out)
			}
			if len(observed) != 1 || observed[0] != cancelled {
				t.Fatalf("observer saw %v, want exactly the returned *Cancelled", observed)
			}
		})
	}
}

// A cancelled Stream's reader returns the cancel, not io.EOF: output cut
// short by the kill never reads as complete.
func TestCancelledStreamReadsTheCancel(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := Command(ctx, Spec{}, "-c", "alias.hang=!f() { echo early; sleep 30; }; f", "hang").Stream(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("early\n"))
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, readErr := io.ReadAll(r)
	var cancelled *Cancelled
	if !errors.As(readErr, &cancelled) || cancelled.Kill.Processes < 2 {
		t.Fatalf("read after the cancel = %v, want a *Cancelled ending at least git and its shell", readErr)
	}
	if err := r.Close(); err != readErr {
		t.Fatalf("Close = %v, want the read's %v", err, readErr)
	}
}

// Closing a Stream before its end is the caller's choice: a git that wrote
// everything and exited 0 closes as nil.
func TestStreamClosedEarlyIsNotAFailure(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	r, err := Command(context.Background(), Spec{}, "version").Stream(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(r, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
}
