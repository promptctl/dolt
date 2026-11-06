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
	var failures []GCFailure
	opener := WithGCFailureObserver(ctx, func(f GCFailure) { failures = append(failures, f) })
	opener, cancel := context.WithCancel(gitcmd.WithObserver(opener, func(c *gitcmd.Cancelled) { observed = append(observed, c) }))
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
	require.Len(t, failures, 1, "a gc its timeout ended is a gc failure too")
	require.ErrorIs(t, failures[0].Err, context.DeadlineExceeded)
	info, err := os.Stat(filepath.Join(localRepo.GitDir, ".dolt-gc-last"))
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), info.ModTime(), time.Minute, "a gc its timeout ended waits a full day")
}

// A gc that fails reaches the opener's GCFailure observer with its exit status
// and output, and stays owed: a Close within the hour leaves it, one after the
// hour runs it again. A gc that succeeds reports nothing and is not owed for a
// day.
func TestGitBlobstore_Close_ReportsAFailedGCAndLeavesItOwed(t *testing.T) {
	requireGitOnPath(t)
	realGit, err := exec.LookPath("git")
	require.NoError(t, err)
	shim := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(shim, "git"), []byte(`#!/bin/sh
for a in "$@"; do [ "$a" = gc ] && [ -e "$GC_FAILS" ] && { echo "gc refused" >&2; exit 3; }; done
exec "`+realGit+`" "$@"
`), 0o755))
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	failsFlag := filepath.Join(t.TempDir(), "fails")
	require.NoError(t, os.WriteFile(failsFlag, nil, 0o644))
	t.Setenv("GC_FAILS", failsFlag)

	ctx := context.Background()
	_, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	var observed []GCFailure
	opener := WithGCFailureObserver(ctx, func(f GCFailure) { observed = append(observed, f) })
	marker := filepath.Join(localRepo.GitDir, ".dolt-gc-last")
	closeOnce := func() {
		bs, err := NewGitBlobstoreWithOptions(opener, localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
		require.NoError(t, err)
		require.NoError(t, bs.Close())
	}

	closeOnce()
	require.Len(t, observed, 1)
	require.Equal(t, localRepo.GitDir, observed[0].Mirror)
	require.Equal(t, 3, observed[0].ExitCode)
	require.Contains(t, observed[0].Output, "gc refused")
	info, err := os.Stat(marker)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(gcRetryInterval-gcInterval), info.ModTime(), time.Minute, "owed again an hour from now")

	closeOnce()
	require.Len(t, observed, 1, "a gc that failed within the hour is not tried again yet")

	anHourOn := info.ModTime().Add(-gcRetryInterval - time.Minute)
	require.NoError(t, os.Chtimes(marker, anHourOn, anHourOn))
	require.NoError(t, os.Remove(failsFlag))
	closeOnce()
	require.Len(t, observed, 1, "a gc that succeeds reports nothing")
	info, err = os.Stat(marker)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), info.ModTime(), time.Minute, "a gc that completed is not owed for a day")
}

// A gc lock Close cannot take, for a reason other than another process
// holding it, is a GCFailure, not a gc skipped in silence on every Close.
func TestGitBlobstore_Close_ReportsAGCLockItCouldNotTake(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	_, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	// A directory where the lock file goes makes opening it fail.
	require.NoError(t, os.Mkdir(filepath.Join(localRepo.GitDir, ".dolt-gc.lock"), 0o755))

	var observed []GCFailure
	opener := WithGCFailureObserver(ctx, func(f GCFailure) { observed = append(observed, f) })
	bs, err := NewGitBlobstoreWithOptions(opener, localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
	require.NoError(t, err)
	require.NoError(t, bs.Close())
	require.Len(t, observed, 1)
	require.ErrorContains(t, observed[0].Err, "take gc lock")
}

// A gc marker Close cannot set is a GCFailure: gc then runs on every later
// Close, and each says so.
func TestGitBlobstore_Close_ReportsAGCMarkerItCouldNotWrite(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	_, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	// A directory where the marker goes makes the write fail.
	require.NoError(t, os.Mkdir(filepath.Join(localRepo.GitDir, ".dolt-gc-last"), 0o755))
	aDayAgo := time.Now().Add(-gcInterval - time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(localRepo.GitDir, ".dolt-gc-last"), aDayAgo, aDayAgo))

	var observed []GCFailure
	opener := WithGCFailureObserver(ctx, func(f GCFailure) { observed = append(observed, f) })
	bs, err := NewGitBlobstoreWithOptions(opener, localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
	require.NoError(t, err)
	require.NoError(t, bs.Close())
	require.Len(t, observed, 1)
	require.Equal(t, localRepo.GitDir, observed[0].Mirror)
	require.ErrorContains(t, observed[0].Err, "set gc marker")
}
