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

package dbfactory

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

func TestFileFactory_CreateDB_SingletonCacheAndBypass(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	t.Run("default uses singleton cache", func(t *testing.T) {
		root := t.TempDir()
		nomsDir := filepath.Join(root, "noms")
		require.NoError(t, os.MkdirAll(nomsDir, 0o755))

		urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)

		db1, _, _, err := CreateDB(ctx, nbf, urlStr, map[string]interface{}{ChunkJournalParam: struct{}{}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = CloseAllLocalDatabases() })

		db2, _, _, err := CreateDB(ctx, nbf, urlStr, map[string]interface{}{ChunkJournalParam: struct{}{}})
		require.NoError(t, err)

		require.True(t, db1 == db2, "expected singleton cache to return same DB instance")
	})

	t.Run("DisableSingletonCacheParam bypasses singleton cache", func(t *testing.T) {
		root := t.TempDir()
		nomsDir := filepath.Join(root, "noms")
		require.NoError(t, os.MkdirAll(nomsDir, 0o755))

		urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
		params := map[string]interface{}{
			ChunkJournalParam:          struct{}{},
			DisableSingletonCacheParam: struct{}{},
		}

		db1, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db1.Close() })

		db2, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db2.Close() })

		require.True(t, db1 != db2, "expected bypass mode to return different DB instances")
	})
}

func TestFileFactory_CreateDB_FailOnJournalLockTimeoutParam(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	root := t.TempDir()
	nomsDir := filepath.Join(root, "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
	params := map[string]interface{}{
		ChunkJournalParam:             struct{}{},
		DisableSingletonCacheParam:    struct{}{},
		FailOnJournalLockTimeoutParam: struct{}{},
	}

	// First open takes the journal manifest lock and holds it until closed.
	db1, _, _, err := CreateDB(ctx, nbf, urlStr, params)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })

	// Second open should fail fast when the fail-on-timeout flag is honored.
	_, _, _, err = CreateDB(ctx, nbf, urlStr, params)
	require.Error(t, err)
	require.ErrorIs(t, err, nbs.ErrDatabaseLocked)
}

// A journal warning names its store by the directory it is on this
// filesystem, the one the store's URL names, unescaped and in the OS's own
// separators, so an embedder can open the store the warning is about.
func TestFileFactory_CreateDB_JournalWarningNamesTheStoreDirectory(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default
	nomsDir := filepath.Join(t.TempDir(), "a store", "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))
	urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
	params := map[string]interface{}{
		ChunkJournalParam:          struct{}{},
		DisableSingletonCacheParam: struct{}{},
	}

	// a record length past the journal's maximum: the tail an interrupted
	// write leaves, which the open recovers from and warns about
	tail := bytes.Repeat([]byte{0xFF}, 8)
	require.NoError(t, os.WriteFile(filepath.Join(nomsDir, strings.Repeat("v", 32)), tail, 0o644))

	var observed []nbs.JournalWarning
	ctx = nbs.WithJournalWarningObserver(ctx, func(w nbs.JournalWarning) { observed = append(observed, w) })
	db, _, _, err := CreateDB(ctx, nbf, urlStr, params)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NotEmpty(t, observed)
	for _, w := range observed {
		require.Equal(t, nomsDir, w.Store)
	}
}
