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

// Package proctree starts a command as a process tree: when the command's
// context is cancelled, the process and every process descended from it are
// ended, not only the process exec started.
//
// os/exec's default cancel kills one process. A child that process started (the
// ssh git runs for git-receive-pack, the git-remote-http it runs for an http
// remote, the real git.exe Git for Windows' launcher runs) keeps running and
// keeps the stdout and stderr pipes open, so Wait blocks until that child exits
// on its own, and the child goes on talking to the network after its caller
// gave up on it.
package proctree

import (
	"errors"
	"os/exec"
	"slices"
	"time"
)

// Kill is what a cancel did to a tree, read once Wait has returned.
type Kill struct {
	// Processes is how many processes the kill ended, the started process
	// included. Zero means the cancel came after the started process had
	// exited, and ended nothing: a process it left running is not in the
	// tree, because once it has exited no process table places anything
	// under it.
	Processes int
	// Survivors is how many of them were still running ExitBound after the
	// kill. Non-zero means the kill did not end the tree. Nil means no count
	// was taken: nothing was killed, or the process table could not be read,
	// and Err says why.
	Survivors *int
	// Err is why ending the tree failed: a process table that could not be
	// read, or a signal or job call the OS refused.
	Err error
}

// ExitBound is how long Settle waits for the processes a kill ended to exit.
// The wait ends when the last of them exits, so only a process that does not
// die (one stuck in uninterruptible I/O) costs the whole bound.
const ExitBound = 5 * time.Second

// exitPoll is how often Settle looks again for a killed process still running.
// It sets how soon Settle notices the last exit, never whether it does.
const exitPoll = 10 * time.Millisecond

// Start starts cmd as a tree and installs the tree's kill as cmd.Cancel, so a
// cancelled cmd.Context ends the whole tree. cmd must have been built by
// exec.CommandContext. Call Settle once Wait has returned.
func Start(cmd *exec.Cmd) (*Tree, error) {
	if cmd.Cancel == nil {
		return nil, errors.New("proctree: command has no context; build it with exec.CommandContext")
	}
	t := &Tree{}
	cmd.Cancel = t.kill
	if err := t.start(cmd); err != nil {
		return nil, err
	}
	return t, nil
}

// Settle reports what the cancel did, if one ended anything, and releases
// what the tree held. Call it once, after Wait has returned. Its readings are
// taken once the killed processes have exited, or ExitBound after the kill,
// so a process still dying is not counted as surviving.
// [LAW:no-ambient-temporal-coupling]
func (t *Tree) Settle() Kill {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.record.Processes > 0 {
		survivors, err := awaitExit(t.running, ExitBound)
		t.record.Survivors = survivors
		t.record.Err = errors.Join(t.record.Err, err)
	}
	t.release()
	return t.record
}

// awaitExit reads running until it reports none, or until bound has passed,
// and returns the last reading. A reading that fails ends the wait with no
// count, never a zero that reads as one.
func awaitExit(running func() (int, error), bound time.Duration) (*int, error) {
	deadline := time.Now().Add(bound)
	for {
		n, err := running()
		if err != nil {
			return nil, err
		}
		if n == 0 || !time.Now().Before(deadline) {
			return &n, nil
		}
		time.Sleep(exitPoll)
	}
}

// row is one process in a process table: its pid, its parent's, when it
// started, and whether it is a zombie, a process that has exited and not yet
// been reaped.
type row struct {
	pid, ppid int
	start     uint64
	zombie    bool
}

// proc identifies one process across readings of the table. A pid alone does
// not: once a process is reaped, its pid can be given to a new one.
type proc struct {
	pid   int
	start uint64
}

// newMembers returns the processes in table whose parent is a member and that
// are not members yet: the next ring of a tree whose members are known. A
// process whose parent exited before the walk reached it has been reparented
// away, and no table can place it in the tree.
func newMembers(table []row, members map[int]bool) []row {
	var found []row
	for _, r := range table {
		if members[r.ppid] && !members[r.pid] {
			found = append(found, r)
		}
	}
	return found
}

// stillRunning counts the procs present in table and not zombies.
func stillRunning(table []row, procs []proc) int {
	n := 0
	for _, r := range table {
		if !r.zombie && slices.Contains(procs, proc{pid: r.pid, start: r.start}) {
			n++
		}
	}
	return n
}
