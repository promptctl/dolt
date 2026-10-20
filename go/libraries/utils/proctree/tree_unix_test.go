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
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// The tree stays in the caller's process group, the foreground group of its
// terminal when it has one, so a git or ssh that prompts on /dev/tty prompts
// instead of stopping on SIGTTIN.
func TestTreeStaysInTheCallersProcessGroup(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), os.Args[0])
	cmd.Env = append(os.Environ(), roleEnv+"=stdin")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close(); _ = cmd.Wait(); tree.Settle() }()
	group, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if group != syscall.Getpgrp() {
		t.Fatalf("the tree's process group is %d, the caller's is %d", group, syscall.Getpgrp())
	}
}
