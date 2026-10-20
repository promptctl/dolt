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

package blobstore

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/gitcmd"
)

// Close's git runs on its opener's values without its opener's cancellation:
// with the opener already cancelled, Close still does its work, and the gc its
// deadline cancels is reported to the observer the opener carried.
func TestGitBlobstore_Close_ReportsACancelledGCToItsOpener(t *testing.T) {
	requireGitOnPath(t)
	realGit, err := exec.LookPath("git")
	require.NoError(t, err)
	// A git whose gc hangs, with a child of its own, until it is killed.
	shim := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(shim, "git"), []byte(`#!/bin/sh
for a in "$@"; do [ "$a" = gc ] && { sleep 30 & wait; }; done
exec "`+realGit+`" "$@"
`), 0o755))
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))

	ctx := context.Background()
	_, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	var observed []*gitcmd.Cancelled
	opener, cancel := context.WithCancel(gitcmd.WithObserver(ctx, func(c *gitcmd.Cancelled) { observed = append(observed, c) }))
	bs, err := NewGitBlobstoreWithOptions(opener, localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
	require.NoError(t, err)
	bs.gcTimeout = 200 * time.Millisecond
	cancel()

	start := time.Now()
	require.NoError(t, bs.Close())
	require.Less(t, time.Since(start), 5*time.Second)
	require.Len(t, observed, 1)
	require.ErrorIs(t, observed[0], context.DeadlineExceeded)
	require.GreaterOrEqual(t, observed[0].Kill.Processes, 2, "the shim and its sleep")
}
