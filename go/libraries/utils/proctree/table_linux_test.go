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
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

// A process reaped between the open and the read of its /proc stat is a
// process that exited, not a table that could not be read: every lit host has
// processes exiting while the table is read.
func TestReapedBetweenOpenAndReadHasVanished(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), roleEnv+"=stdin")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Open("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	defer stat.Close()
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(stat)
	if err == nil {
		t.Fatal("read the stat of a reaped process: the fixture does not open the window")
	}
	if !vanished(err) {
		t.Fatalf("vanished(%v) = false: a reaped process reads as a table that could not be read", err)
	}
}
