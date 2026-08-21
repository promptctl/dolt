// Copyright 2022 Dolthub, Inc.
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
//
// The change: the PNG-histogram helpers (plotNodeSizeDistribution,
// plotIntHistogram) and this module's only gonum.org/v1/plot import are
// removed. The sample statistics and the text summaries below are unchanged.

package tree

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

type Samples []int

func (s Samples) Summary() string {
	f := "mean: %8.2f \t stddev: %8.2f \t p50: %5d \t p90: %5d \t p99: %5d \t p99.9: %5d \t p100: %5d"
	p50, p90, p99, p999, p100 := s.percentiles()
	return fmt.Sprintf(f, s.mean(), s.stdDev(), p50, p90, p99, p999, p100)
}

func (s Samples) count() float64 {
	return float64(len(s))
}

func (s Samples) sum() (total float64) {
	for _, v := range s {
		total += float64(v)
	}
	return
}

func (s Samples) mean() float64 {
	return s.sum() / float64(len(s))
}

func (s Samples) stdDev() float64 {
	var acc float64
	u := s.mean()
	for _, v := range s {
		d := float64(v) - u
		acc += d * d
	}
	return math.Sqrt(acc / s.count())
}

func (s Samples) percentiles() (p50, p90, p99, p999, p100 int) {
	sort.Ints(s)
	l := len(s)
	p50 = s[l/2]
	p90 = s[(l*9)/10]
	p99 = s[(l*99)/100]
	p999 = s[(l*999)/1000]
	p100 = s[l-1]
	return
}

func PrintTreeSummaryByLevel(t *testing.T, nd *Node, ns NodeStore) {
	ctx := context.Background()

	sizeByLevel := make([]Samples, nd.Level()+1)
	cardByLevel := make([]Samples, nd.Level()+1)
	err := WalkNodes(ctx, nd, ns, func(ctx context.Context, nd *Node) error {
		lvl := nd.Level()
		sizeByLevel[lvl] = append(sizeByLevel[lvl], nd.Size())
		cardByLevel[lvl] = append(cardByLevel[lvl], int(nd.count))
		return nil
	})
	require.NoError(t, err)

	fmt.Println("pre-edit map Summary: ")
	fmt.Println("| Level | count | avg Size \t  p50 \t  p90 \t p100 | avg card \t  p50 \t  p90 \t p100 |")
	for i := nd.Level(); i >= 0; i-- {
		sizes, cards := sizeByLevel[i], cardByLevel[i]
		sp50, _, sp90, _, sp100 := sizes.percentiles()
		cp50, _, cp90, _, cp100 := cards.percentiles()
		fmt.Printf("| %5d | %5d | %8.2f \t %4d \t %4d \t %4d | %8.2f \t %4d \t %4d \t %4d |\n",
			i, len(cards), sizes.mean(), sp50, sp90, sp100, cards.mean(), cp50, cp90, cp100)
	}
	fmt.Println()
}

func measureTreeNodes(nd *Node, ns NodeStore) (Samples, error) {
	ctx := context.Background()
	data := make(Samples, 0, 1024)
	err := WalkNodes(ctx, nd, ns, func(ctx context.Context, nd *Node) error {
		data = append(data, nd.Size())
		return nil
	})
	return data, err
}
