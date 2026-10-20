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

import "golang.org/x/sys/unix"

// processTable reads every process's pid and parent pid from the kernel.
func processTable() ([]row, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, err
	}
	table := make([]row, 0, len(procs))
	for _, p := range procs {
		table = append(table, row{
			pid:    int(p.Proc.P_pid),
			ppid:   int(p.Eproc.Ppid),
			start:  uint64(p.Proc.P_starttime.Sec)*1e6 + uint64(p.Proc.P_starttime.Usec),
			zombie: p.Proc.P_stat == sZomb,
		})
	}
	return table, nil
}

// sZomb is SZOMB from <sys/proc.h>: the process has exited and awaits reaping.
const sZomb = 5
