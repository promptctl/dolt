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

// NOTICE (Apache License 2.0, section 4(b)): this file was added in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not part
// of upstream github.com/dolthub/dolt. What it pins, and why, is recorded in
// the patch ledger that README.lit-fork.md at the root of this fork points to.

package nbs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
	fslock "github.com/promptctl/primitives/filelock"
)

// lockEvents records what a JournalLockObserver was told, and whether the
// directory's LOCK was held at each moment it was told.
type lockEvents struct {
	t      *testing.T
	path   string
	events []string
}

func (e *lockEvents) observer() JournalLockObserver {
	return JournalLockObserver{
		Taken:    func() { e.events = append(e.events, "taken held="+e.held()) },
		Released: func() { e.events = append(e.events, "released held="+e.held()) },
	}
}

// held probes LOCK from a handle of its own: a second flock on the file fails
// while the store's is held, even within this process.
func (e *lockEvents) held() string {
	probe := fslock.New(e.path)
	if err := probe.TryLock(); err != nil {
		return "yes"
	}
	require.NoError(e.t, probe.Unlock())
	return "no"
}

func openObservedJournalingStore(t *testing.T, dir string, lock JournalLock) (*lockEvents, *NomsBlockStore, error) {
	events := &lockEvents{t: t, path: filepath.Join(dir, lockFileName)}
	ctx := WithJournalLockObserver(context.Background(), events.observer())
	st, err := NewLocalJournalingStoreWithOptions(ctx, types.Format_Default.VersionString(), dir, NewUnlimitedMemQuotaProvider(), false, nil, JournalingStoreOptions{Lock: lock})
	return events, st, err
}

func TestJournalLockObserverIsToldWhenTheLockIsTakenAndLetGo(t *testing.T) {
	dir := t.TempDir()
	events, st, err := openObservedJournalingStore(t, dir, JournalLockOrFail)
	require.NoError(t, err)
	require.Equal(t, []string{"taken held=yes"}, events.events)
	require.NoError(t, st.Close())
	require.Equal(t, []string{"taken held=yes", "released held=no"}, events.events)
}

func TestJournalLockObserverHearsNothingFromAStoreThatNeverHoldsTheLock(t *testing.T) {
	dir := t.TempDir()
	_, holder, err := openObservedJournalingStore(t, dir, JournalLockOrFail)
	require.NoError(t, err)
	defer holder.Close()

	refused, _, err := openObservedJournalingStore(t, dir, JournalLockOrFail)
	require.ErrorIs(t, err, ErrDatabaseLocked)
	require.Empty(t, refused.events, "a store that lost the lock")

	readOnly, st, err := openObservedJournalingStore(t, dir, JournalReadOnly)
	require.NoError(t, err)
	require.NoError(t, st.Close())
	require.Empty(t, readOnly.events, "a read-only store")
}

func TestJournalLockObserverIsToldTheLockWentWhenTheManifestOpenFails(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, manifestFileName), []byte("not a manifest"), 0o644))
	events, _, err := openObservedJournalingStore(t, dir, JournalLockOrFail)
	require.Error(t, err)
	require.Equal(t, []string{"taken held=yes", "released held=no"}, events.events)
}
