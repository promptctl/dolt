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

//go:build !darwin && !linux && !windows

package proctree

import (
	"fmt"
	"os/exec"
	"runtime"
	"sync"
)

// Tree is unsupported here: lit ships for darwin, linux and windows, and a
// git started without a tree would outlive its cancel.
type Tree struct {
	mu     sync.Mutex
	record Kill
}

func (t *Tree) start(*exec.Cmd) error {
	return fmt.Errorf("proctree: process trees are not implemented on %s", runtime.GOOS)
}

func (t *Tree) kill() error           { return nil }
func (t *Tree) running() (int, error) { return 0, nil }
func (t *Tree) release()              {}
