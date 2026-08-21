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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// NOTICE (Apache License 2.0, section 4(b)): this file was modified in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not the
// upstream github.com/dolthub/dolt version. What changed, why, and what would
// let the change be dropped are recorded in the patch ledger that
// README.lit-fork.md at the root of this fork points to.

package main

import (
	"flag"
	"log"
	"os"

	fslock "github.com/promptctl/primitives/filelock"
)

func main() {
	flag.Parse()

	if flag.NArg() < 3 {
		log.Fatalln("Not enough arguments")
	}

	// Clobber manifest file at flag.Arg(1) with contents at flag.Arg(2) after taking lock of file flag.Arg(0)
	lockFile := flag.Arg(0)
	manifestFile := flag.Arg(1)
	manifestContents := flag.Arg(2)

	// lock released by closing l.
	lck := fslock.New(lockFile)
	err := lck.TryLock()
	if err == fslock.ErrLocked {
		return
	}
	if err != nil {
		log.Fatalln(err)
	}

	defer lck.Unlock()

	m, err := os.Create(manifestFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer m.Close()

	if _, err = m.WriteString(manifestContents); err != nil {
		log.Fatalln(err)
	}
}
