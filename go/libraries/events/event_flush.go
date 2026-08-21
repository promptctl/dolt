// Copyright 2019 Dolthub, Inc.
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

// NOTICE (Apache License 2.0, section 4(b)): this file was modified in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not the
// upstream github.com/dolthub/dolt version. What changed, why, and what would
// let the change be dropped are recorded in the patch ledger that
// README.lit-fork.md at the root of this fork points to.

package events

import (
	"context"
	"errors"
	"io/fs"

	fslock "github.com/promptctl/primitives/filelock"
	"google.golang.org/protobuf/proto"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var (
	// ErrEventsDataDir occurs when events are trying to be  flushed, but the events data directory
	// does not yet exist
	ErrEventsDataDir = errors.New("unable to flush, events data directory does not exist")

	// ErrFileLocked occurs if the current file or dir is locked for processing
	ErrFileLocked = errors.New("file is currently locked")

	// errInvalidFile occurs if the filename fails the CheckingFunc
	errInvalidFile = errors.New("unable to flush, invalid file")
)

// flushCB is the signature of the callback used to process event files
type flushCB func(ctx context.Context, path string) error

// Flusher flushes events to a destination
type Flusher interface {
	Flush(ctx context.Context) error
}

type FileFlusher struct {
	emitter Emitter
	fbp     *FileBackedProc
}

func NewFileFlusher(fs filesys.Filesys, userHomeDir string, doltDir string, emitter Emitter) *FileFlusher {
	fbp := NewFileBackedProc(fs, userHomeDir, doltDir, MD5FileNamer, CheckFilenameMD5)

	if exists := fbp.EventsDirExists(); !exists {
		panic(ErrEventsDataDir)
	}

	return &FileFlusher{emitter: emitter, fbp: fbp}
}

func (f FileFlusher) Flush(ctx context.Context) error {
	fs := f.fbp.GetFileSys()

	evtsDir := f.fbp.GetEventsDirPath()

	err := f.lockAndFlush(ctx, fs, evtsDir, f.fbp.LockPath)
	if err != nil {
		return err
	}

	return nil
}

// flush has the function signature of the flushCb type
// and sends events data to the events server
func (f FileFlusher) flush(ctx context.Context, path string) error {
	fs := f.fbp.GetFileSys()

	data, err := fs.ReadFile(path)
	if err != nil {
		return err
	}

	isFileValid, err := f.fbp.CheckingFunc(data, path)

	if isFileValid && err == nil {
		req := &eventsapi.LogEventsRequest{}

		if err := proto.Unmarshal(data, req); err != nil {
			return err
		}

		if err := f.emitter.LogEventsRequest(ctx, req); err != nil {
			return err
		}

		if err := fs.DeleteFile(path); err != nil {
			return err
		}

		return nil
	}

	return errInvalidFile
}

var _ Flusher = &FileFlusher{}

// lockAndFlush locks the given lockPath and passes the flushCB to the filesys' Iter method
func (f FileFlusher) lockAndFlush(ctx context.Context, fsys filesys.Filesys, dirPath string, lockPath string) error {
	fsLock := filesys.CreateFilesysLock(fsys, lockPath)

	isUnlocked, err := fsLock.TryLock()
	defer func() error {
		err := fsLock.Unlock()
		if err != nil {
			return err
		}
		return nil
	}()

	if err != nil {
		if errors.Is(err, fslock.ErrLocked) {
			return ErrFileLocked
		}
		return err
	}

	if !isUnlocked {
		return nil
	}

	var returnErr error
	iterErr := fsys.Iter(dirPath, false, func(path string, size int64, isDir bool) (stop bool) {
		if err := f.flush(ctx, path); err != nil {
			if errors.Is(err, errInvalidFile) {
				// ignore invalid files found in the events directory
				return false
			} else if _, isPathError := err.(*fs.PathError); isPathError {
				// The lock file on windows has this issue, skip this file
				// We can't use errors.Is because fs.PathError doesn't implement Is
				return false
			}
			returnErr = err
			return true
		}

		return false
	})

	if iterErr != nil {
		return iterErr
	} else if returnErr != nil {
		return returnErr
	}

	return nil
}
