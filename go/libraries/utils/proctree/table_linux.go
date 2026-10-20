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
	"bytes"
	"errors"
	"io/fs"
	"os"
	"strconv"
	"syscall"
)

// processTable reads every process's pid and parent pid from /proc. A process
// that exits between the directory read and its stat read is skipped: it is in
// no tree any more.
func processTable() ([]row, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	table := make([]row, 0, len(entries))
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // not a process directory
		}
		stat, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if vanished(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		r, err := parseStat(pid, stat)
		if err != nil {
			return nil, err
		}
		table = append(table, r)
	}
	return table, nil
}

// vanished reports whether err, from reading a process's /proc entry, means
// the process has exited: ENOENT once its directory is gone, ESRCH when it was
// reaped between the open and the read (the kernel's proc_single_show). Such
// a row is not in the table; any other error is a table that could not be
// read.
func vanished(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

// parseStat reads the state, ppid and start time from a /proc/<pid>/stat line,
// "pid (comm) state ppid ...". comm is the executable's name and may hold
// spaces and parentheses, so the fields are read after its last ')'.
func parseStat(pid int, stat []byte) (row, error) {
	close := bytes.LastIndexByte(stat, ')')
	if close < 0 {
		return row{}, &os.PathError{Op: "parse", Path: "/proc/" + strconv.Itoa(pid) + "/stat", Err: errors.New("no ')' after comm")}
	}
	fields := bytes.Fields(stat[close+1:])
	// fields[0] is stat's field 3 (state), fields[1] field 4 (ppid), and
	// fields[19] field 22 (starttime, in clock ticks since boot).
	if len(fields) < 20 {
		return row{}, &os.PathError{Op: "parse", Path: "/proc/" + strconv.Itoa(pid) + "/stat", Err: errors.New("fewer than 22 fields")}
	}
	ppid, err := strconv.Atoi(string(fields[1]))
	if err != nil {
		return row{}, &os.PathError{Op: "parse", Path: "/proc/" + strconv.Itoa(pid) + "/stat", Err: err}
	}
	start, err := strconv.ParseUint(string(fields[19]), 10, 64)
	if err != nil {
		return row{}, &os.PathError{Op: "parse", Path: "/proc/" + strconv.Itoa(pid) + "/stat", Err: err}
	}
	return row{pid: pid, ppid: ppid, start: start, zombie: string(fields[0]) == "Z"}, nil
}
