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

// Package gitcmd is the one way lit and Dolt start git. Every git it starts
// runs with core.longpaths on, and runs as a process tree: when its context is
// cancelled, git and everything git started (ssh, git-remote-http, the real
// git.exe behind Git for Windows' launcher) are ended, and the error says
// what the cancel ended.
package gitcmd

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/proctree"
)

// LongPaths lets Git for Windows open paths past MAX_PATH (260 characters).
// Every command passes it; other platforms ignore the key. git hands -c
// settings to the processes it starts on this side of a fetch or push, but not
// to an upload-pack or receive-pack it starts in a local repository, so a
// caller naming one of those passes LongPaths to it itself.
const LongPaths = "core.longpaths=true"

// outputBound is how long git's output may stay open once the run's context
// is done and git has exited. Every run is unbounded while its context lives,
// so an uncancelled git's output is read to the end; a done context bounds it.
const outputBound = time.Second

// CancelBound is the longest a cancelled run takes to return once git itself
// has exited: proctree's ExitBound for the processes the kill ended, then
// outputBound for their output. A caller that bounds its own shutdown gives a
// cancelled git at least this, or the run's *Cancelled never reaches its
// observer. [LAW:no-ambient-temporal-coupling]
const CancelBound = proctree.ExitBound + outputBound

// Spec is how a git command runs, apart from its arguments.
type Spec struct {
	// Dir is the working directory; "" is the caller's.
	Dir string
	// Env is the environment; nil is the caller's.
	Env []string
	// Stdin is git's input; nil is none.
	Stdin io.Reader
}

// Cmd is a git command, run once by one of Output, CombinedOutput, Run or
// Stream. It has no Start and no Wait: every run pairs the tree's start with
// its one Wait inside this package.
type Cmd struct {
	ctx context.Context
	cmd *exec.Cmd
}

// Command builds "git -c core.longpaths=true <args...>".
func Command(ctx context.Context, spec Spec, args ...string) *Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", LongPaths}, args...)...) //nolint:gosec // callers pass git plumbing arguments
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	cmd.Stdin = spec.Stdin
	return &Cmd{ctx: ctx, cmd: cmd}
}

// Output runs git and returns its stdout. A git that exits non-zero leaves
// its stderr in the error's *exec.ExitError, as exec.Cmd.Output does.
func (c *Cmd) Output() ([]byte, error) {
	var stdout, stderr bytes.Buffer
	err := c.Run(&stdout, &stderr)
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitErr.Stderr = stderr.Bytes()
	}
	return stdout.Bytes(), err
}

// CombinedOutput runs git and returns its stdout and stderr interleaved.
func (c *Cmd) CombinedOutput() ([]byte, error) {
	var out bytes.Buffer
	err := c.Run(&out, &out)
	return out.Bytes(), err
}

// Run runs git with its stdout and stderr written to the given writers; a
// nil writer discards.
func (c *Cmd) Run(stdout, stderr io.Writer) error {
	out, err := copyOutputs(c.cmd, stdout, stderr)
	if err != nil {
		return err
	}
	tree, err := c.start(out)
	if err != nil {
		return err
	}
	return c.wait(tree, out)
}

// Stream starts git and returns its stdout as it is written, with stderr
// written to stderr. A read after git has ended returns io.EOF if the run
// succeeded and the run's error if not, so a cancelled git's cut-short output
// never reads as complete. Close stops reading and returns how git ended.
func (c *Cmd) Stream(stderr io.Writer) (io.ReadCloser, error) {
	read, write := io.Pipe()
	out, err := copyOutputs(c.cmd, write, stderr)
	if err != nil {
		return nil, err
	}
	tree, err := c.start(out)
	if err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		err := c.wait(tree, out)
		_ = write.CloseWithError(err)
		done <- err
	}()
	return &stream{read: read, done: done}, nil
}

type stream struct {
	read *io.PipeReader
	done <-chan error
}

func (s *stream) Read(p []byte) (int, error) { return s.read.Read(p) }

// Close closes the stream and returns how git ended. A git still writing
// when the caller closes ends on SIGPIPE, as it would writing to a closed
// pipe; the copy that the close cut short is the caller's choice, not a
// failure of git's.
func (s *stream) Close() error {
	_ = s.read.Close()
	if err := <-s.done; !errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return nil
}

// start starts git as a tree, then closes this process's write ends of the
// output pipes: from here on only git and what it starts hold them, so a copy
// reaches EOF when the last of those exits.
func (c *Cmd) start(out *outputs) (*proctree.Tree, error) {
	tree, err := proctree.Start(c.cmd)
	out.closeParentEnds()
	if err != nil {
		out.abandon()
		return nil, err
	}
	return tree, nil
}

// wait waits for git, settles its tree, and waits for its output, every run
// the same way. A run its context ended is handed to the context's observer
// and returned. Otherwise the run is exec's result, or the first copy error,
// with the output complete.
func (c *Cmd) wait(tree *proctree.Tree, out *outputs) error {
	err := c.cmd.Wait()
	kill := tree.Settle()
	cut, copyErr := out.drain(c.ctx)
	if cancelled := cancelledRun(c.ctx.Err(), err, kill, cut); cancelled != nil {
		observerOf(c.ctx)(cancelled)
		return cancelled
	}
	// A failed git's error already explains why its output stopped.
	return cmp.Or(err, copyErr)
}

// cancelledRun is the run its context ended, or nil when the context did not
// end it. A run whose context is done did not finish when the kill ended
// git's process tree, when its output was cut outputBound after the context
// was done, or when git exited non-zero. The last is a git the interrupt
// reached before the kill did: Ctrl-C signals the terminal's foreground
// process group, git included, so git can die of SIGINT with nothing left for
// the kill to end. Wait reports a Cancel failure only for a process that
// exited cleanly, and a cancelled git died of the kill, so the *Cancelled
// carries the kill's record. [LAW:no-silent-failure]
func cancelledRun(cause, exit error, kill proctree.Kill, cut bool) *Cancelled {
	if cause == nil || (exit == nil && kill.Processes == 0 && !cut) {
		return nil
	}
	return &Cancelled{Cause: cause, Exit: exit, Kill: kill, OutputCut: cut}
}

// outputs is the pipes gitcmd copies git's stdout and stderr through. Each
// writer gets an *os.File write end, so exec copies nothing itself and Wait
// returns when git exits; the copies here decide when the output is done.
type outputs struct {
	parentEnds []*os.File
	readEnds   []*os.File
	copies     chan error
}

// copyOutputs gives cmd a pipe for each non-nil writer, one shared pipe when
// stdout and stderr are the same writer, as exec does, and starts copying
// each into its writer. A copy closes its read end when it stops, so a git
// still writing to a writer that refused the output ends on SIGPIPE rather
// than blocking on a full pipe.
func copyOutputs(cmd *exec.Cmd, stdout, stderr io.Writer) (*outputs, error) {
	out := &outputs{copies: make(chan error, 2)}
	pipe := func(w io.Writer) (*os.File, error) {
		if w == nil {
			return nil, nil
		}
		read, write, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		out.parentEnds = append(out.parentEnds, write)
		out.readEnds = append(out.readEnds, read)
		go func() {
			_, err := io.Copy(w, read)
			_ = read.Close()
			out.copies <- err
		}()
		return write, nil
	}
	stdoutEnd, err := pipe(stdout)
	if err != nil {
		return nil, err
	}
	stderrEnd := stdoutEnd
	if !sameWriter(stdout, stderr) {
		if stderrEnd, err = pipe(stderr); err != nil {
			out.closeParentEnds()
			out.abandon()
			return nil, err
		}
	}
	// A nil *os.File in an io.Writer is not a nil Writer, and exec would
	// write to it; leave the field nil instead, which exec reads as discard.
	if stdoutEnd != nil {
		cmd.Stdout = stdoutEnd
	}
	if stderrEnd != nil {
		cmd.Stderr = stderrEnd
	}
	return out, nil
}

// sameWriter reports whether a and b are one writer. Comparing two
// interfaces panics when their dynamic type is not comparable; such writers
// are not the same writer here.
func sameWriter(a, b io.Writer) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return a != nil && a == b
}

func (o *outputs) closeParentEnds() {
	for _, f := range o.parentEnds {
		_ = f.Close()
	}
}

// drain waits for every copy to stop, with no bound while ctx lives. Once ctx
// is done it waits at most outputBound more, then closes the read ends, which
// stops every copy still running. It reports whether it had to (cut), and the
// first error a copy that ran to its end returned.
func (o *outputs) drain(ctx context.Context) (cut bool, copyErr error) {
	done := ctx.Done()
	var expired <-chan time.Time
	for pending := len(o.readEnds); pending > 0; {
		select {
		case err := <-o.copies:
			pending--
			if !cut && copyErr == nil {
				copyErr = err
			}
		case <-done:
			done = nil
			timer := time.NewTimer(outputBound)
			defer timer.Stop()
			expired = timer.C
		case <-expired:
			expired = nil
			cut = true
			o.closeReadEnds()
		}
	}
	return cut, copyErr
}

// abandon ends the copies of a git that never started.
func (o *outputs) abandon() {
	o.closeReadEnds()
	for range o.readEnds {
		<-o.copies
	}
}

func (o *outputs) closeReadEnds() {
	for _, f := range o.readEnds {
		_ = f.Close()
	}
}

// Cancelled is a git run that its context ended, and what ending it did: the
// process tree it killed, the output it cut, or both.
type Cancelled struct {
	// Cause is the context's error: context.Canceled or
	// context.DeadlineExceeded.
	Cause error
	// Exit is exec's error for the ended git.
	Exit error
	// Kill is what ending git's process tree did.
	Kill proctree.Kill
	// OutputCut is whether gitcmd stopped reading git's output because it
	// was still open outputBound after the context was done and the tree had
	// ended: a process outside the tree held it, or one of Kill's survivors.
	// What was read before the cut was returned; the rest was not.
	OutputCut bool
}

func (e *Cancelled) Error() string {
	msg := fmt.Sprintf("git cancelled (%v): ended %d process(es)", e.Cause, e.Kill.Processes)
	if s := e.Kill.Survivors; s != nil && *s > 0 {
		msg += fmt.Sprintf(", %d still running", *s)
	}
	if e.Kill.Err != nil {
		msg += fmt.Sprintf(", kill failed: %v", e.Kill.Err)
	}
	if e.OutputCut {
		msg += fmt.Sprintf(", output cut: still open %s after the tree ended", outputBound)
	}
	if e.Exit != nil {
		msg += ": " + e.Exit.Error()
	}
	return msg
}

// Unwrap keeps errors.Is(err, context.DeadlineExceeded) and
// errors.As(err, **exec.ExitError) answering as they did for exec's error.
func (e *Cancelled) Unwrap() []error {
	return []error{e.Cause, e.Exit}
}

// WithObserver returns ctx carrying observe, which every git run under it
// that a cancel ends is handed to, before the run returns. It is how the owner
// of a unit of work sees each cancel, including one whose error is handled
// rather than returned, or flattened to text on its way up: a Dolt fetch or
// push reaches lit through a SQL driver that keeps only an error's message.
// [LAW:nothing-unseen]
func WithObserver(ctx context.Context, observe func(*Cancelled)) context.Context {
	return context.WithValue(ctx, observerKey{}, observe)
}

type observerKey struct{}

// observerOf is ctx's observer, or one that observes nothing.
func observerOf(ctx context.Context) func(*Cancelled) {
	if observe, ok := ctx.Value(observerKey{}).(func(*Cancelled)); ok {
		return observe
	}
	return func(*Cancelled) {}
}
