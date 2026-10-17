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

// NOTICE: this file does not exist upstream. It was added to this fork in 2026
// by Brandon Fryslie for the links-issue-tracker (`lit`) project as the
// regression test for patch 7; see the patch ledger README.lit-fork.md at the
// root of this fork points to.

package blobstore

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

// cacheRefs lists every ref in the local cache repository as "<name> <oid>".
func cacheRefs(t *testing.T, ctx context.Context, runner *git.Runner) []string {
	t.Helper()
	out, err := runner.Run(ctx, git.RunOptions{}, "for-each-ref", "--format=%(refname) %(objectname)")
	require.NoError(t, err)
	return strings.Fields(strings.ReplaceAll(strings.TrimSpace(string(out)), " ", "="))
}

// A closed instance leaves exactly one ref behind: the last-known ref, at the
// remote head it read. Its own UUID refs are gone.
func TestGitBlobstore_Close_LeavesOnlyTheLastKnownRef(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remoteRepo, localRepo, localRunner := newRemoteAndLocalRepos(t, ctx)
	head, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
	require.NoError(t, err)
	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, bs.Close())

	require.Equal(t, []string{LastKnownRemoteRef("origin", DoltDataRef) + "=" + string(head)}, cacheRefs(t, ctx, localRunner))
}

// A closed writer publishes what it pushed, not the remote state it fetched
// before pushing: the cache must still hold the objects it just sent.
func TestGitBlobstore_Close_PublishesWhatTheInstancePushed(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remoteRepo, localRepo, localRunner := newRemoteAndLocalRepos(t, ctx)
	before, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin", Identity: testIdentity()})
	require.NoError(t, err)
	// The manifest, because only a manifest write pushes; any other key is
	// queued until the next one.
	payload := []byte("written by this instance\n")
	_, err = bs.Put(ctx, "manifest", int64(len(payload)), bytes.NewReader(payload))
	require.NoError(t, err)
	require.NoError(t, bs.Close())

	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	pushed, err := git.NewGitAPIImpl(remoteRunner).ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	require.NotEqual(t, before, pushed, "the Put must have moved the remote")
	require.Equal(t, []string{LastKnownRemoteRef("origin", DoltDataRef) + "=" + string(pushed)}, cacheRefs(t, ctx, localRunner))
}

// An instance that never synced has no view of the remote, so closing it
// leaves an earlier instance's published state where it was.
func TestGitBlobstore_Close_WithoutSyncKeepsAnEarlierLastKnownRef(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remoteRepo, localRepo, localRunner := newRemoteAndLocalRepos(t, ctx)
	head, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	reader, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
	require.NoError(t, err)
	_, err = reader.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	idle, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{RemoteName: "origin"})
	require.NoError(t, err)
	require.NoError(t, idle.Close())

	require.Equal(t, []string{LastKnownRemoteRef("origin", DoltDataRef) + "=" + string(head)}, cacheRefs(t, ctx, localRunner))
}
