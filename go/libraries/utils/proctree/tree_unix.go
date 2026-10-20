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

//go:build darwin || linux

package proctree

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

// Tree is a started command and, once killed, the processes it had started.
//
// The command stays in its caller's process group and session. A tree in a
// group of its own could be ended with one group kill, but a git or ssh in a
// background group that prompts on /dev/tty (an https credential, an ssh
// passphrase or host key) is stopped by SIGTTIN instead of prompting.
type Tree struct {
	mu      sync.Mutex
	process *os.Process
	// descendants are the processes the kill ended besides the root, which
	// Wait reaps.
	descendants []proc
	record      Kill
}

func (t *Tree) start(cmd *exec.Cmd) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := cmd.Start(); err != nil {
		return err
	}
	t.process = cmd.Process
	return nil
}

// kill stops the tree from the root down, so no member can fork once it has
// been found, then kills every member. The root is signalled through its
// *os.Process, which knows whether Wait has already reaped it; a descendant is
// signalled by pid only once it is stopped, and a stopped process cannot exit
// and give its pid away.
func (t *Tree) kill() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.process.Signal(syscall.SIGSTOP); err != nil {
		// os.ErrProcessDone: Wait reaped the command before the cancel ran.
		// exec reads it as "nothing to cancel".
		return err
	}
	descendants, freezeErr := freeze(t.process.Pid)
	t.descendants = descendants
	killErr := t.process.Signal(syscall.SIGKILL)
	for _, p := range descendants {
		killErr = errors.Join(killErr, signal(p.pid, syscall.SIGKILL))
	}
	t.record.Processes = 1 + len(descendants)
	t.record.Err = errors.Join(freezeErr, killErr)
	return t.record.Err
}

// freeze returns every process descended from root, each stopped. It reads
// the process table until a read finds no process whose parent is a member
// and that is not one yet.
func freeze(root int) ([]proc, error) {
	var descendants []proc
	known := map[int]bool{root: true}
	var errs error
	for {
		table, err := processTable()
		if err != nil {
			return descendants, errors.Join(errs, err)
		}
		ring := newMembers(table, known)
		if len(ring) == 0 {
			return descendants, errs
		}
		for _, r := range ring {
			known[r.pid] = true
			switch err := signal(r.pid, syscall.SIGSTOP); {
			case err == nil:
				descendants = append(descendants, proc{pid: r.pid, start: r.start})
			case !errors.Is(err, syscall.ESRCH):
				errs = errors.Join(errs, err)
			}
		}
	}
}

func signal(pid int, sig syscall.Signal) error {
	if err := syscall.Kill(pid, sig); err != nil {
		return &os.SyscallError{Syscall: "kill " + sig.String(), Err: err}
	}
	return nil
}

// running counts the killed descendants still running. The root is not among
// them: Wait has reaped it.
func (t *Tree) running() (int, error) {
	table, err := processTable()
	if err != nil {
		return 0, err
	}
	return stillRunning(table, t.descendants), nil
}

func (t *Tree) release() {}
