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
// regression test for patch 9; see the patch ledger README.lit-fork.md at the
// root of this fork points to.

package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/types"
)

// TestSetHeadIfAt is the lease's contract: the head moves only while it is
// the one named, and the comparison is against the head being replaced, not
// the head the caller's Dataset last read.
func (suite *DatabaseSuite) TestSetHeadIfAt() {
	ctx := context.Background()

	// |a| <- |b| <- |c|
	ds, err := suite.db.GetDataset(ctx, "ds1")
	suite.Require().NoError(err)
	ds, err = CommitValue(ctx, suite.db, ds, types.String("a"))
	suite.Require().NoError(err)
	a := mustHeadAddr(ds)
	ds, err = CommitValue(ctx, suite.db, ds, types.String("b"))
	suite.Require().NoError(err)
	b := mustHeadAddr(ds)
	ds, err = CommitValue(ctx, suite.db, ds, types.String("c"))
	suite.Require().NoError(err)
	c := mustHeadAddr(ds)

	// The lease names the head: a non-fast-forward move lands.
	ds, err = suite.db.SetHeadIfAt(ctx, ds, c, a, "")
	suite.Require().NoError(err)
	suite.Equal(a, mustHeadAddr(ds))

	// The lease names a commit the head is not at: refused, head unchanged.
	_, err = suite.db.SetHeadIfAt(ctx, ds, b, c, "")
	suite.ErrorIs(err, ErrStaleInfo)
	ds, err = suite.db.GetDataset(ctx, "ds1")
	suite.Require().NoError(err)
	suite.Equal(a, mustHeadAddr(ds))

	// Another writer moves the head after the caller read it: the caller's
	// Dataset still says |a|, the lease names |a|, and it is refused.
	stale := ds
	_, err = suite.db.SetHead(ctx, ds, b, "")
	suite.Require().NoError(err)
	_, err = suite.db.SetHeadIfAt(ctx, stale, a, c, "")
	suite.ErrorIs(err, ErrStaleInfo)
	ds, err = suite.db.GetDataset(ctx, "ds1")
	suite.Require().NoError(err)
	suite.Equal(b, mustHeadAddr(ds))

	// A head already at the new address satisfies any lease: the retry of
	// an update that landed, or a peer that pushed the same commit.
	ds, err = suite.db.SetHeadIfAt(ctx, ds, c, b, "")
	suite.Require().NoError(err)
	suite.Equal(b, mustHeadAddr(ds))

	// A dataset with no head matches no lease, and is not created.
	empty, err := suite.db.GetDataset(ctx, "ds2")
	suite.Require().NoError(err)
	_, err = suite.db.SetHeadIfAt(ctx, empty, a, c, "")
	suite.ErrorIs(err, ErrStaleInfo)
	empty, err = suite.db.GetDataset(ctx, "ds2")
	suite.Require().NoError(err)
	suite.False(empty.HasHead())
}
