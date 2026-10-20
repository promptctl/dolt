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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Tree is a started command and the Job Object every process it starts
// inherits. Git for Windows' launcher (cmd\git.exe) runs the real
// mingw64\bin\git.exe as its child, and that git runs git-remote-https and
// ssh as its own, so ending the launcher alone ends none of the work.
type Tree struct {
	mu sync.Mutex
	// root is the started process, opened to see whether it has exited: a
	// cancel ends the tree only while it runs, as on POSIX.
	root   windows.Handle
	job    windows.Handle
	failed bool
	record Kill
}

// start creates the process suspended, so it cannot start a child before it
// is in the job, assigns it to a new job, then resumes it. A job without
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: a git that exits on its own leaves
// whatever it meant to leave running (fsmonitor--daemon), as it always has.
func (t *Tree) start(cmd *exec.Cmd) error {
	t.mu.Lock()
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	if err := cmd.Start(); err != nil {
		t.mu.Unlock()
		return err
	}
	root, job, err := jobFor(uint32(cmd.Process.Pid))
	if err != nil {
		// Unlock before reaping: Wait waits on exec's ctx watcher, which may
		// be inside kill, waiting on this mutex.
		t.failed = true
		t.mu.Unlock()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	t.root, t.job = root, job
	t.mu.Unlock()
	return nil
}

// jobFor puts the suspended process pid in a new job and resumes it. It
// returns the process, opened while it is suspended and so still pid's, and
// the job.
func jobFor(pid uint32) (root, job windows.Handle, err error) {
	root, err = windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.SYNCHRONIZE, false, pid)
	if err != nil {
		return 0, 0, fmt.Errorf("proctree: open process %d: %w", pid, err)
	}
	job, err = windows.CreateJobObject(nil, nil)
	if err != nil {
		_ = windows.CloseHandle(root)
		return 0, 0, fmt.Errorf("proctree: create job object: %w", err)
	}
	if err := windows.AssignProcessToJobObject(job, root); err != nil {
		_ = windows.CloseHandle(job)
		_ = windows.CloseHandle(root)
		return 0, 0, fmt.Errorf("proctree: assign process %d to its job: %w", pid, err)
	}
	if err := resumeThreads(pid); err != nil {
		_ = windows.CloseHandle(job)
		_ = windows.CloseHandle(root)
		return 0, 0, err
	}
	return root, job, nil
}

// resumeThreads resumes the threads of process pid. A process created
// suspended has one, its main thread; os/exec does not hand its handle out, so
// it is found in a thread snapshot.
func resumeThreads(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return fmt.Errorf("proctree: snapshot threads: %w", err)
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	resumed := 0
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if err != nil {
			return fmt.Errorf("proctree: open thread %d of process %d: %w", entry.ThreadID, pid, err)
		}
		_, err = windows.ResumeThread(thread)
		_ = windows.CloseHandle(thread)
		if err != nil {
			return fmt.Errorf("proctree: resume thread %d of process %d: %w", entry.ThreadID, pid, err)
		}
		resumed++
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return fmt.Errorf("proctree: walk threads: %w", err)
	}
	if resumed == 0 {
		return fmt.Errorf("proctree: process %d has no thread to resume", pid)
	}
	return nil
}

// kill terminates every process in the job, if the started process is still
// running. One that has exited has ended its run, and what it left running in
// the job it left on purpose.
func (t *Tree) kill() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failed {
		return os.ErrProcessDone
	}
	switch event, err := windows.WaitForSingleObject(t.root, 0); {
	case err != nil:
		return fmt.Errorf("proctree: wait on process: %w", err)
	case event == windows.WAIT_OBJECT_0:
		// The started process has exited; Wait has not noticed yet.
		return os.ErrProcessDone
	}
	active, err := activeProcesses(t.job)
	if err != nil {
		return err
	}
	t.record.Processes = active
	if err := windows.TerminateJobObject(t.job, 1); err != nil {
		t.record.Err = fmt.Errorf("proctree: terminate job: %w", err)
	}
	return t.record.Err
}

// running counts the job's processes still running.
func (t *Tree) running() (int, error) {
	return activeProcesses(t.job)
}

func (t *Tree) release() {
	for _, h := range []*windows.Handle{&t.job, &t.root} {
		if *h != 0 {
			_ = windows.CloseHandle(*h)
			*h = 0
		}
	}
}

// jobAccounting is JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, which x/sys/windows
// does not declare.
type jobAccounting struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func activeProcesses(job windows.Handle) (int, error) {
	var info jobAccounting
	if err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil); err != nil {
		return 0, fmt.Errorf("proctree: read job accounting: %w", err)
	}
	return int(info.ActiveProcesses), nil
}
