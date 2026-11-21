// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package traces

import (
	"math"
	"sort"
)

// bipartiteEdge represents a weighted compatibility edge between left node
// index `left` (`0..numLeft-1`) and right node index `right` (`0..numRight-1`).
// Callers must pass weight >= 0 so the initial DAG graph has non-positive edge
// costs and admits O(V + E) Johnson potential initialization without negative cycles.
type bipartiteEdge struct {
	left   int
	right  int
	weight int64
}

type groupRightKey struct {
	group int
	right int
}

// cost2Pair is a 16-byte lexicographic pair (a, b) kept <= 16 bytes to avoid
// Go -race checkptrAlignment/duffcopy overhead on >16B structs.
type cost2Pair struct {
	a int64
	b int64
}

func addCost2(x, y cost2Pair) cost2Pair {
	return cost2Pair{a: x.a + y.a, b: x.b + y.b}
}

func subCost2(x, y cost2Pair) cost2Pair {
	return cost2Pair{a: x.a - y.a, b: x.b - y.b}
}

func lessCost2(x, y cost2Pair) bool {
	if x.a != y.a {
		return x.a < y.a
	}
	return x.b < y.b
}

// cost3 is used only as a fallback on sparse/incomplete graphs whose weights
// exceed int64 packing bounds.
type cost3 struct {
	miss int64
	negW int64
	negT int64
}

func addCost3(x, y cost3) cost3 {
	return cost3{miss: x.miss + y.miss, negW: x.negW + y.negW, negT: x.negT + y.negT}
}

func subCost3(x, y cost3) cost3 {
	return cost3{miss: x.miss - y.miss, negW: x.negW - y.negW, negT: x.negT - y.negT}
}

func lessCost3(x, y cost3) bool {
	if x.miss != y.miss {
		return x.miss < y.miss
	}
	if x.negW != y.negW {
		return x.negW < y.negW
	}
	return x.negT < y.negT
}

// solveBipartiteMatching computes a deterministic maximum-cardinality,
// maximum-weight bipartite matching via an exact hybrid solver (packed/16B
// Hungarian with convex-dual initialization, Hopcroft-Karp perfect-matching
// certification, sparse CSR Hungarian, bounded right-capacity expansion, and
// flat-SoA generation-stamped CSR SSAP).
//
//   - rightCap (optional): capacity of right node r; defaults to 1 if nil.
//   - leftGroup (optional): when non-nil and len(leftGroup) == numLeft, left
//     items sharing the same group ID may share a right node ONLY up to 1 total
//     unit across that group.
//   - Ties among equal-weight maximum-cardinality matchings break FIFO using
//     supermodular tie weight (numLeft-i)*(numRight-j).
func solveBipartiteMatching(numLeft, numRight int, rightCap []int, leftGroup []int, edges []bipartiteEdge) []int {
	assign := make([]int, numLeft)
	for i := range assign {
		assign[i] = -1
	}
	if numLeft <= 0 || numRight <= 0 || len(edges) == 0 {
		return assign
	}

	dedup := sortAndDedupEdgesFast(numLeft, numRight, rightCap, edges)
	if len(dedup) == 0 {
		return assign
	}

	// Determine whether all active right nodes have capacity == 1.
	allRightCapOne := true
	if rightCap != nil {
		for _, e := range dedup {
			if rightCap[e.right] != 1 {
				allRightCapOne = false
				break
			}
		}
	}

	// Determine whether any (group, right) pair actually has >= 2 incident edges.
	var midKeys []groupRightKey
	var groupCount map[int]int
	hasMultiGroup := false
	if len(leftGroup) == numLeft {
		groupCount = make(map[int]int)
		for _, g := range leftGroup {
			groupCount[g]++
		}
		pairCounts := make(map[groupRightKey]int)
		for _, e := range dedup {
			g := leftGroup[e.left]
			if groupCount[g] > 1 {
				k := groupRightKey{group: g, right: e.right}
				pairCounts[k]++
			}
		}
		for k, cnt := range pairCounts {
			if cnt >= 2 {
				midKeys = append(midKeys, k)
			}
		}
		if len(midKeys) > 0 {
			hasMultiGroup = true
			sort.Slice(midKeys, func(i, j int) bool {
				if midKeys[i].group != midKeys[j].group {
					return midKeys[i].group < midKeys[j].group
				}
				return midKeys[i].right < midKeys[j].right
			})
		}
	}

	// Branch 1: Unit-capacity right nodes without multi-edge group conflicts.
	if allRightCapOne && !hasMultiGroup {
		solveUncapacitated(numLeft, numRight, numLeft, numRight, nil, false, dedup, assign)
		return assign
	}

	// Branch 2A: Expand bounded capacitated right nodes (without group conflicts)
	// into unit-capacity copies so dense workloads (e.g. 1002x501 cap=2) use Hungarian.
	if !hasMultiGroup && numRight > 16 {
		degR := make([]int, numRight)
		for _, e := range dedup {
			degR[e.right]++
		}
		effCap := make([]int, numRight)
		sumCap := 0
		for r := 0; r < numRight; r++ {
			capVal := 0
			if r < len(rightCap) {
				capVal = rightCap[r]
			}
			if capVal > degR[r] {
				capVal = degR[r]
			}
			if capVal > 0 {
				effCap[r] = capVal
				sumCap += capVal
			}
		}
		if sumCap == 0 {
			return assign
		}
		if sumCap > numRight && sumCap <= 2*numLeft && int64(numLeft)*int64(sumCap) <= 4_000_000 {
			expEdgeCount := 0
			for _, e := range dedup {
				expEdgeCount += effCap[e.right]
			}
			if expEdgeCount <= 4_000_000 {
				rightStart := make([]int, numRight)
				copyOrigR := make([]int, sumCap)
				cur := 0
				for r := 0; r < numRight; r++ {
					rightStart[r] = cur
					for k := 0; k < effCap[r]; k++ {
						copyOrigR[cur+k] = r
					}
					cur += effCap[r]
				}
				expEdges := make([]bipartiteEdge, 0, expEdgeCount)
				for _, e := range dedup {
					rs := rightStart[e.right]
					c := effCap[e.right]
					for k := 0; k < c; k++ {
						expEdges = append(expEdges, bipartiteEdge{
							left:   e.left,
							right:  rs + k,
							weight: e.weight,
						})
					}
				}
				solveUncapacitated(numLeft, sumCap, numLeft, numRight, copyOrigR, true, expEdges, assign)
				return assign
			}
		}
	}

	// Branch 2B: Flat-SoA generation-stamped CSR SSAP for general capacities & leftGroup.
	solveFastCSRSSAP(numLeft, numRight, rightCap, leftGroup, midKeys, dedup, assign)
	return assign
}

// sortAndDedupEdgesFast checks in O(E) if edges are already valid and strictly sorted
// by (left, right) with weight >= 0 (returning edges directly with zero allocations),
// otherwise filters, sorts (2-pass radix sort for E > 64), and deduplicates keeping max weight.
func sortAndDedupEdgesFast(numLeft, numRight int, rightCap []int, edges []bipartiteEdge) []bipartiteEdge {
	alreadySorted := true
	prevL, prevR := -1, -1
	for _, e := range edges {
		if e.left < 0 || e.left >= numLeft || e.right < 0 || e.right >= numRight || e.weight < 0 {
			alreadySorted = false
			break
		}
		if rightCap != nil && (e.right >= len(rightCap) || rightCap[e.right] <= 0) {
			alreadySorted = false
			break
		}
		if e.left < prevL || (e.left == prevL && e.right <= prevR) {
			alreadySorted = false
			break
		}
		prevL, prevR = e.left, e.right
	}
	if alreadySorted {
		return edges
	}

	filtered := make([]bipartiteEdge, 0, len(edges))
	for _, e := range edges {
		if e.left < 0 || e.left >= numLeft || e.right < 0 || e.right >= numRight {
			continue
		}
		if rightCap != nil && (e.right >= len(rightCap) || rightCap[e.right] <= 0) {
			continue
		}
		if e.weight < 0 {
			e.weight = 0
		}
		filtered = append(filtered, e)
	}
	if len(filtered) == 0 {
		return nil
	}

	if len(filtered) <= 64 {
		sort.Slice(filtered, func(i, j int) bool {
			if filtered[i].left != filtered[j].left {
				return filtered[i].left < filtered[j].left
			}
			if filtered[i].right != filtered[j].right {
				return filtered[i].right < filtered[j].right
			}
			return filtered[i].weight > filtered[j].weight
		})
		dedup := filtered[:0]
		for _, e := range filtered {
			if len(dedup) > 0 && dedup[len(dedup)-1].left == e.left && dedup[len(dedup)-1].right == e.right {
				continue
			}
			dedup = append(dedup, e)
		}
		return dedup
	}

	// 2-pass stable counting/radix sort: first by right, then by left.
	cntR := make([]int, numRight)
	for _, e := range filtered {
		cntR[e.right]++
	}
	offR := make([]int, numRight)
	pos := 0
	for r := 0; r < numRight; r++ {
		offR[r] = pos
		pos += cntR[r]
	}
	tmp := make([]bipartiteEdge, len(filtered))
	for _, e := range filtered {
		idx := offR[e.right]
		offR[e.right] = idx + 1
		tmp[idx] = e
	}

	cntL := make([]int, numLeft)
	for _, e := range tmp {
		cntL[e.left]++
	}
	offL := make([]int, numLeft)
	pos = 0
	for l := 0; l < numLeft; l++ {
		offL[l] = pos
		pos += cntL[l]
	}
	sorted := filtered // reuse filtered buffer
	for _, e := range tmp {
		idx := offL[e.left]
		offL[e.left] = idx + 1
		sorted[idx] = e
	}

	dedup := sorted[:0]
	for _, e := range sorted {
		n := len(dedup)
		if n > 0 && dedup[n-1].left == e.left && dedup[n-1].right == e.right {
			if e.weight > dedup[n-1].weight {
				dedup[n-1].weight = e.weight
			}
			continue
		}
		dedup = append(dedup, e)
	}
	return dedup
}

func canPackWeightAndTie(maxW int64, origNumL, origNumR, maxCard, m int) (tieScale int64, ok bool) {
	if origNumL <= 0 || origNumR <= 0 || maxCard <= 0 || m <= 0 {
		return 0, false
	}
	if int64(origNumL) > math.MaxInt64/int64(origNumR) {
		return 0, false
	}
	maxTie := int64(origNumL) * int64(origNumR)
	if int64(maxCard) > (math.MaxInt64-1)/maxTie {
		return 0, false
	}
	tieScale = int64(maxCard)*maxTie + 1
	if maxW > 0 {
		if maxW > (math.MaxInt64-maxTie)/tieScale {
			return 0, false
		}
	}
	maxEdgeAbs := maxW*tieScale + maxTie
	safetyFactor := int64(8) * int64(m+2)
	if maxEdgeAbs > math.MaxInt64/safetyFactor {
		return 0, false
	}
	return tieScale, true
}

func canPackDenseWithSkip(maxW int64, origNumL, origNumR, activeL, activeR, maxCard, m int) (tieScale int64, skipPenalty int64, ok bool) {
	tieScale, ok = canPackWeightAndTie(maxW, origNumL, origNumR, maxCard, m)
	if !ok {
		return 0, 0, false
	}
	maxTie := int64(origNumL) * int64(origNumR)
	maxEdgeAbs := maxW*tieScale + maxTie
	if int64(maxCard) > (math.MaxInt64-1)/maxEdgeAbs {
		return 0, 0, false
	}
	skipPenalty = int64(maxCard)*maxEdgeAbs + 1
	totalAbs := skipPenalty + maxEdgeAbs
	if totalAbs < skipPenalty {
		return 0, 0, false
	}
	// Reserve headroom for max |u0Tie| as well as skipPenalty + maxEdgeAbs.
	maxU0Est := int64(origNumL+origNumR)*int64(origNumL) + int64(activeL) + int64(activeR)
	if totalAbs > math.MaxInt64-maxU0Est {
		return 0, 0, false
	}
	totalAbs += maxU0Est
	safetyFactor := int64(8) * int64(m+2)
	if totalAbs > math.MaxInt64/safetyFactor {
		return 0, 0, false
	}
	return tieScale, skipPenalty, true
}

func solveUncapacitated(
	numLeft, numRight int,
	origNumL, origNumR int,
	copyOrigR []int,
	expanded bool,
	dedup []bipartiteEdge,
	assign []int,
) {
	// Compact degree-0 left and right nodes while preserving original coordinates for tie-breaking.
	leftMap := make([]int, numLeft)
	for i := range leftMap {
		leftMap[i] = -1
	}
	rightMap := make([]int, numRight)
	for j := range rightMap {
		rightMap[j] = -1
	}
	for _, e := range dedup {
		leftMap[e.left] = 0
		rightMap[e.right] = 0
	}
	activeL := 0
	for l := 0; l < numLeft; l++ {
		if leftMap[l] == 0 {
			leftMap[l] = activeL
			activeL++
		}
	}
	activeR := 0
	for r := 0; r < numRight; r++ {
		if rightMap[r] == 0 {
			rightMap[r] = activeR
			activeR++
		}
	}
	if activeL == 0 || activeR == 0 {
		return
	}

	origLeft := make([]int, activeL)
	for l := 0; l < numLeft; l++ {
		if leftMap[l] >= 0 {
			origLeft[leftMap[l]] = l
		}
	}
	origRight := make([]int, activeR)
	for r := 0; r < numRight; r++ {
		if rightMap[r] >= 0 {
			if copyOrigR != nil {
				origRight[rightMap[r]] = copyOrigR[r]
			} else {
				origRight[rightMap[r]] = r
			}
		}
	}

	var maxW int64
	for _, e := range dedup {
		if e.weight > maxW {
			maxW = e.weight
		}
	}

	maxCard := activeL
	if activeR < maxCard {
		maxCard = activeR
	}

	mDim := activeL
	if activeR > mDim {
		mDim = activeR
	}

	isDense := int64(activeL)*int64(activeR) <= 4_000_000 && int64(len(dedup))*4 >= int64(activeL)*int64(activeR)

	if isDense {
		isComplete := len(dedup) == activeL*activeR
		canPadComplete := isComplete && mDim <= 2*maxCard
		hasPerfect := isComplete && activeL == activeR
		if !isComplete && activeL == activeR {
			hasPerfect = hasPerfectMatchingDense(activeL, activeR, leftMap, rightMap, dedup)
		}
		if canPadComplete || hasPerfect {
			tieScale, packOK := canPackWeightAndTie(maxW, origNumL, origNumR, maxCard, mDim)
			solveHungarianPerfectOrComplete(
				activeL, activeR, mDim, origNumL, origNumR,
				leftMap, rightMap, origLeft, origRight,
				expanded, isComplete, packOK, tieScale,
				dedup, assign,
			)
			return
		}
		tieScale, skipPenalty, packSkipOK := canPackDenseWithSkip(maxW, origNumL, origNumR, activeL, activeR, maxCard, activeR)
		packOK := packSkipOK
		if !packOK {
			tieScale, packOK = canPackWeightAndTie(maxW, origNumL, origNumR, maxCard, activeR)
		}
		solveHungarianDense(
			activeL, activeR, origNumL, origNumR,
			leftMap, rightMap, origLeft, origRight,
			expanded, packSkipOK, packOK, tieScale, skipPenalty, dedup, assign,
		)
		return
	}

	tieScale, packOK := canPackWeightAndTie(maxW, origNumL, origNumR, maxCard, activeL+activeR)
	solveSparseHungarian(
		activeL, activeR, origNumL, origNumR,
		leftMap, rightMap, origLeft, origRight,
		packOK, tieScale, dedup, assign,
	)
}

// hasPerfectMatchingDense certifies in O(sqrt(V) * E) via Hopcroft-Karp whether
// an activeL == activeR dense graph admits a size-activeL perfect matching.
func hasPerfectMatchingDense(activeL, activeR int, leftMap, rightMap []int, dedup []bipartiteEdge) bool {
	rowHead := make([]int, activeL+1)
	for _, e := range dedup {
		rowHead[leftMap[e.left]+1]++
	}
	for i := 1; i <= activeL; i++ {
		rowHead[i] += rowHead[i-1]
	}
	rowCols := make([]int, len(dedup))
	cur := make([]int, activeL)
	copy(cur, rowHead[:activeL])
	for _, e := range dedup {
		al := leftMap[e.left]
		rowCols[cur[al]] = rightMap[e.right]
		cur[al]++
	}

	pairU := make([]int, activeL)
	pairV := make([]int, activeR)
	for i := range pairU {
		pairU[i] = -1
	}
	for j := range pairV {
		pairV[j] = -1
	}

	matched := 0
	for u := 0; u < activeL; u++ {
		for k := rowHead[u]; k < rowHead[u+1]; k++ {
			v := rowCols[k]
			if pairV[v] < 0 {
				pairU[u] = v
				pairV[v] = u
				matched++
				break
			}
		}
	}
	if matched == activeL {
		return true
	}

	dist := make([]int, activeL)
	queue := make([]int, activeL)
	const infDist = math.MaxInt32

	bfs := func() bool {
		head, tail := 0, 0
		for u := 0; u < activeL; u++ {
			if pairU[u] < 0 {
				dist[u] = 0
				queue[tail] = u
				tail++
			} else {
				dist[u] = infDist
			}
		}
		found := false
		for head < tail {
			u := queue[head]
			head++
			du := dist[u]
			for k := rowHead[u]; k < rowHead[u+1]; k++ {
				v := rowCols[k]
				pu := pairV[v]
				if pu < 0 {
					found = true
				} else if dist[pu] == infDist {
					dist[pu] = du + 1
					queue[tail] = pu
					tail++
				}
			}
		}
		return found
	}

	var dfs func(u int) bool
	dfs = func(u int) bool {
		du := dist[u]
		for k := rowHead[u]; k < rowHead[u+1]; k++ {
			v := rowCols[k]
			pu := pairV[v]
			if pu < 0 || (dist[pu] == du+1 && dfs(pu)) {
				pairV[v] = u
				pairU[u] = v
				return true
			}
		}
		dist[u] = infDist
		return false
	}

	for bfs() {
		for u := 0; u < activeL; u++ {
			if pairU[u] < 0 && dfs(u) {
				matched++
			}
		}
	}
	return matched == activeL
}

func computeConvexDualU0(
	activeL, activeR, origNumL, origNumR int,
	origLeft []int,
	expanded bool,
) []int64 {
	u0 := make([]int64, activeL)
	nL := int64(origNumL)
	nR := int64(origNumR)
	for i := 0; i < activeL; i++ {
		a := nL - int64(origLeft[i])
		if expanded {
			u0[i] = -((nR * a * (a + 1)) / (2 * nL))
		} else {
			u0[i] = -((a*(a+1))/2 + (nR-nL)*a)
		}
	}
	return u0
}

func solveHungarianPerfectOrComplete(
	activeL, activeR, m, origNumL, origNumR int,
	leftMap, rightMap, origLeft, origRight []int,
	expanded, isComplete, packOK bool,
	tieScale int64,
	dedup []bipartiteEdge,
	assign []int,
) {
	u0Tie := computeConvexDualU0(activeL, activeR, origNumL, origNumR, origLeft, expanded)

	var hasEdge []bool
	if !isComplete {
		hasEdge = make([]bool, m*m)
		for _, e := range dedup {
			hasEdge[leftMap[e.left]*m+rightMap[e.right]] = true
		}
	}

	p := make([]int, m+1)
	for j := range p {
		p[j] = -1
	}
	rowMatched := make([]bool, m)
	way := make([]int, m+1)
	used := make([]bool, m+1)
	hasMinv := make([]bool, m+1)

	if packOK {
		costs := make([]int64, m*m)
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			costs[al*m+ar] = -(e.weight*tieScale + tie)
		}
		u := make([]int64, m)
		copy(u, u0Tie)
		v := make([]int64, m+1)

		for j := 0; j < m; j++ {
			best := int64(0)
			hasBest := false
			for i := 0; i < m; i++ {
				if isComplete || i >= activeL || j >= activeR || hasEdge[i*m+j] {
					cand := costs[i*m+j] - u[i]
					if !hasBest || cand < best {
						best = cand
						hasBest = true
					}
				}
			}
			if hasBest {
				v[j] = best
			}
		}

		// Initial O(N^2) zero-reduced-cost greedy pass.
		for i := 0; i < m; i++ {
			base := i * m
			ui := u[i]
			for j := 0; j < m; j++ {
				if p[j] < 0 && (isComplete || i >= activeL || j >= activeR || hasEdge[base+j]) {
					if costs[base+j]-ui-v[j] == 0 {
						p[j] = i
						rowMatched[i] = true
						break
					}
				}
			}
		}

		minv := make([]int64, m+1)
		for startRow := 0; startRow < m; startRow++ {
			if rowMatched[startRow] {
				continue
			}
			p[m] = startRow
			j0 := m
			for j := 0; j <= m; j++ {
				used[j] = false
				hasMinv[j] = false
				way[j] = m
			}
			for {
				used[j0] = true
				i0 := p[j0]
				base := i0 * m
				ui0 := u[i0]
				delta := int64(0)
				j1 := -1
				for j := 0; j < m; j++ {
					if used[j] {
						continue
					}
					if isComplete || i0 >= activeL || j >= activeR || hasEdge[base+j] {
						cur := costs[base+j] - ui0 - v[j]
						if !hasMinv[j] || cur < minv[j] {
							minv[j] = cur
							hasMinv[j] = true
							way[j] = j0
						}
					}
					if hasMinv[j] && (j1 < 0 || minv[j] < delta) {
						delta = minv[j]
						j1 = j
					}
				}
				if j1 < 0 {
					break
				}
				for j := 0; j <= m; j++ {
					if used[j] {
						u[p[j]] += delta
						if j < m {
							v[j] -= delta
						}
					} else if hasMinv[j] {
						minv[j] -= delta
					}
				}
				j0 = j1
				if p[j0] < 0 {
					break
				}
			}
			if p[j0] < 0 {
				for j0 != m {
					j1 := way[j0]
					p[j0] = p[j1]
					j0 = j1
				}
				rowMatched[startRow] = true
			}
		}
	} else {
		costs := make([]cost2Pair, m*m)
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			costs[al*m+ar] = cost2Pair{a: -e.weight, b: -tie}
		}
		u := make([]cost2Pair, m)
		for i := 0; i < activeL; i++ {
			u[i] = cost2Pair{a: 0, b: u0Tie[i]}
		}
		v := make([]cost2Pair, m+1)

		for j := 0; j < m; j++ {
			var best cost2Pair
			hasBest := false
			for i := 0; i < m; i++ {
				if isComplete || i >= activeL || j >= activeR || hasEdge[i*m+j] {
					cand := subCost2(costs[i*m+j], u[i])
					if !hasBest || lessCost2(cand, best) {
						best = cand
						hasBest = true
					}
				}
			}
			if hasBest {
				v[j] = best
			}
		}

		for i := 0; i < m; i++ {
			base := i * m
			ui := u[i]
			for j := 0; j < m; j++ {
				if p[j] < 0 && (isComplete || i >= activeL || j >= activeR || hasEdge[base+j]) {
					if subCost2(costs[base+j], addCost2(ui, v[j])) == (cost2Pair{}) {
						p[j] = i
						rowMatched[i] = true
						break
					}
				}
			}
		}

		minv := make([]cost2Pair, m+1)
		for startRow := 0; startRow < m; startRow++ {
			if rowMatched[startRow] {
				continue
			}
			p[m] = startRow
			j0 := m
			for j := 0; j <= m; j++ {
				used[j] = false
				hasMinv[j] = false
				way[j] = m
			}
			for {
				used[j0] = true
				i0 := p[j0]
				base := i0 * m
				ui0 := u[i0]
				var delta cost2Pair
				j1 := -1
				for j := 0; j < m; j++ {
					if used[j] {
						continue
					}
					if isComplete || i0 >= activeL || j >= activeR || hasEdge[base+j] {
						cur := subCost2(costs[base+j], addCost2(ui0, v[j]))
						if !hasMinv[j] || lessCost2(cur, minv[j]) {
							minv[j] = cur
							hasMinv[j] = true
							way[j] = j0
						}
					}
					if hasMinv[j] && (j1 < 0 || lessCost2(minv[j], delta)) {
						delta = minv[j]
						j1 = j
					}
				}
				if j1 < 0 {
					break
				}
				for j := 0; j <= m; j++ {
					if used[j] {
						u[p[j]] = addCost2(u[p[j]], delta)
						if j < m {
							v[j] = subCost2(v[j], delta)
						}
					} else if hasMinv[j] {
						minv[j] = subCost2(minv[j], delta)
					}
				}
				j0 = j1
				if p[j0] < 0 {
					break
				}
			}
			if p[j0] < 0 {
				for j0 != m {
					j1 := way[j0]
					p[j0] = p[j1]
					j0 = j1
				}
				rowMatched[startRow] = true
			}
		}
	}

	for j := 0; j < activeR; j++ {
		if p[j] >= 0 && p[j] < activeL {
			assign[origLeft[p[j]]] = origRight[j]
		}
	}
}

func solveHungarianDense(
	activeL, activeR, origNumL, origNumR int,
	leftMap, rightMap, origLeft, origRight []int,
	expanded, packSkipOK, packOK bool,
	tieScale, skipPenalty int64,
	dedup []bipartiteEdge,
	assign []int,
) {
	exists := make([]bool, activeL*activeR)
	p := make([]int, activeR+1)
	for j := range p {
		p[j] = -1
	}
	rowMatched := make([]bool, activeL)
	way := make([]int, activeR+1)
	used := make([]bool, activeR+1)
	hasMinv := make([]bool, activeR+1)

	if packSkipOK {
		costs := make([]int64, activeL*activeR)
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			exists[al*activeR+ar] = true
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			costs[al*activeR+ar] = -(skipPenalty + e.weight*tieScale + tie)
		}
		u := computeConvexDualU0(activeL, activeR, origNumL, origNumR, origLeft, expanded)
		v := make([]int64, activeR)
		var maxV int64
		hasMaxV := false
		for j := 0; j < activeR; j++ {
			var best int64
			hasBest := false
			for i := 0; i < activeL; i++ {
				if exists[i*activeR+j] {
					cand := costs[i*activeR+j] - u[i]
					if !hasBest || cand < best {
						best = cand
						hasBest = true
					}
				}
			}
			if hasBest {
				v[j] = best
				if !hasMaxV || best > maxV {
					maxV = best
					hasMaxV = true
				}
			}
		}
		if hasMaxV {
			for j := range v {
				v[j] -= maxV
			}
			for i := range u {
				u[i] += maxV
			}
		}
		for i := 0; i < activeL; i++ {
			base := i * activeR
			var best int64
			hasBest := false
			for j := 0; j < activeR; j++ {
				if exists[base+j] {
					cand := costs[base+j] - v[j]
					if !hasBest || cand < best {
						best = cand
						hasBest = true
					}
				}
			}
			if hasBest {
				if best > 0 {
					best = 0
				}
				u[i] = best
			} else {
				u[i] = 0
			}
		}

		// Initial zero-reduced-cost greedy pass.
		rowCol := make([]int, activeL)
		for i := range rowCol {
			rowCol[i] = -1
		}
		for i := 0; i < activeL; i++ {
			base := i * activeR
			ui := u[i]
			for j := 0; j < activeR; j++ {
				if p[j] < 0 && exists[base+j] && costs[base+j]-ui-v[j] == 0 {
					p[j] = i
					rowMatched[i] = true
					rowCol[i] = j
					break
				}
			}
		}

		// Enforce complementary slackness v[j] == 0 on all unmatched columns in O(E).
		freeColQ := make([]int, 0, activeR)
		for j := 0; j < activeR; j++ {
			if p[j] < 0 && v[j] != 0 {
				v[j] = 0
				freeColQ = append(freeColQ, j)
			}
		}
		for head := 0; head < len(freeColQ); head++ {
			j := freeColQ[head]
			for i := 0; i < activeL; i++ {
				base := i * activeR
				if exists[base+j] && u[i] > costs[base+j] {
					u[i] = costs[base+j]
					if rowMatched[i] {
						c := rowCol[i]
						p[c] = -1
						rowMatched[i] = false
						rowCol[i] = -1
						if v[c] != 0 {
							v[c] = 0
							freeColQ = append(freeColQ, c)
						}
					}
				}
			}
		}

		minv := make([]int64, activeR)
		for startRow := 0; startRow < activeL; startRow++ {
			if rowMatched[startRow] {
				continue
			}
			p[activeR] = startRow
			j0 := activeR
			for j := 0; j <= activeR; j++ {
				used[j] = false
				if j < activeR {
					hasMinv[j] = false
				}
				way[j] = activeR
			}
			dummySlack := -u[startRow]
			dummySrcCol := activeR

			for {
				used[j0] = true
				i0 := p[j0]
				base := i0 * activeR
				ui0 := u[i0]
				var bestColMinv int64
				j1 := -1
				for j := 0; j < activeR; j++ {
					if used[j] {
						continue
					}
					if exists[base+j] {
						cur := costs[base+j] - ui0 - v[j]
						if !hasMinv[j] || cur < minv[j] {
							minv[j] = cur
							hasMinv[j] = true
							way[j] = j0
						}
					}
					if hasMinv[j] && (j1 < 0 || minv[j] < bestColMinv) {
						bestColMinv = minv[j]
						j1 = j
					}
				}
				if j1 < 0 || dummySlack < bestColMinv {
					delta := dummySlack
					for j := 0; j <= activeR; j++ {
						if used[j] {
							u[p[j]] += delta
							if j < activeR {
								v[j] -= delta
							}
						}
					}
					if dummySrcCol != activeR {
						curCol := dummySrcCol
						for curCol != activeR {
							prev := way[curCol]
							p[curCol] = p[prev]
							curCol = prev
						}
					}
					break
				}

				delta := bestColMinv
				for j := 0; j <= activeR; j++ {
					if used[j] {
						u[p[j]] += delta
						if j < activeR {
							v[j] -= delta
						}
					} else if j < activeR && hasMinv[j] {
						minv[j] -= delta
					}
				}
				dummySlack -= delta
				j0 = j1
				if p[j0] < 0 {
					curCol := j0
					for curCol != activeR {
						prev := way[curCol]
						p[curCol] = p[prev]
						curCol = prev
					}
					break
				}
				candDummy := -u[p[j0]]
				if candDummy < dummySlack {
					dummySlack = candDummy
					dummySrcCol = j0
				}
			}
		}
	} else if packOK {
		skipCost := cost2Pair{a: 1, b: 0}
		costs := make([]cost2Pair, activeL*activeR)
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			exists[al*activeR+ar] = true
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			costs[al*activeR+ar] = cost2Pair{a: 0, b: -(e.weight*tieScale + tie)}
		}
		u0Tie := computeConvexDualU0(activeL, activeR, origNumL, origNumR, origLeft, expanded)
		u := make([]cost2Pair, activeL)
		for i := 0; i < activeL; i++ {
			u[i] = cost2Pair{a: 0, b: u0Tie[i]}
		}
		v := make([]cost2Pair, activeR)
		var maxV cost2Pair
		hasMaxV := false
		for j := 0; j < activeR; j++ {
			var best cost2Pair
			hasBest := false
			for i := 0; i < activeL; i++ {
				if exists[i*activeR+j] {
					cand := subCost2(costs[i*activeR+j], u[i])
					if !hasBest || lessCost2(cand, best) {
						best = cand
						hasBest = true
					}
				}
			}
			if hasBest {
				v[j] = best
				if !hasMaxV || lessCost2(maxV, best) {
					maxV = best
					hasMaxV = true
				}
			}
		}
		if hasMaxV {
			for j := range v {
				v[j] = subCost2(v[j], maxV)
			}
			for i := range u {
				u[i] = addCost2(u[i], maxV)
			}
		}
		for i := 0; i < activeL; i++ {
			base := i * activeR
			best := skipCost
			for j := 0; j < activeR; j++ {
				if exists[base+j] {
					cand := subCost2(costs[base+j], v[j])
					if lessCost2(cand, best) {
						best = cand
					}
				}
			}
			u[i] = best
		}

		// Initial zero-reduced-cost greedy pass.
		rowCol := make([]int, activeL)
		for i := range rowCol {
			rowCol[i] = -1
		}
		for i := 0; i < activeL; i++ {
			base := i * activeR
			ui := u[i]
			for j := 0; j < activeR; j++ {
				if p[j] < 0 && exists[base+j] && subCost2(costs[base+j], addCost2(ui, v[j])) == (cost2Pair{}) {
					p[j] = i
					rowMatched[i] = true
					rowCol[i] = j
					break
				}
			}
		}

		// Enforce complementary slackness v[j] == 0 on all unmatched columns in O(E).
		freeColQ := make([]int, 0, activeR)
		for j := 0; j < activeR; j++ {
			if p[j] < 0 && v[j] != (cost2Pair{}) {
				v[j] = cost2Pair{}
				freeColQ = append(freeColQ, j)
			}
		}
		for head := 0; head < len(freeColQ); head++ {
			j := freeColQ[head]
			for i := 0; i < activeL; i++ {
				base := i * activeR
				if exists[base+j] && lessCost2(costs[base+j], u[i]) {
					u[i] = costs[base+j]
					if rowMatched[i] {
						c := rowCol[i]
						p[c] = -1
						rowMatched[i] = false
						rowCol[i] = -1
						if v[c] != (cost2Pair{}) {
							v[c] = cost2Pair{}
							freeColQ = append(freeColQ, c)
						}
					}
				}
			}
		}

		minv := make([]cost2Pair, activeR)
		for startRow := 0; startRow < activeL; startRow++ {
			if rowMatched[startRow] {
				continue
			}
			p[activeR] = startRow
			j0 := activeR
			for j := 0; j <= activeR; j++ {
				used[j] = false
				if j < activeR {
					hasMinv[j] = false
				}
				way[j] = activeR
			}
			dummySlack := subCost2(skipCost, u[startRow])
			dummySrcCol := activeR

			for {
				used[j0] = true
				i0 := p[j0]
				base := i0 * activeR
				ui0 := u[i0]
				var bestColMinv cost2Pair
				j1 := -1
				for j := 0; j < activeR; j++ {
					if used[j] {
						continue
					}
					if exists[base+j] {
						cur := subCost2(costs[base+j], addCost2(ui0, v[j]))
						if !hasMinv[j] || lessCost2(cur, minv[j]) {
							minv[j] = cur
							hasMinv[j] = true
							way[j] = j0
						}
					}
					if hasMinv[j] && (j1 < 0 || lessCost2(minv[j], bestColMinv)) {
						bestColMinv = minv[j]
						j1 = j
					}
				}
				if j1 < 0 || lessCost2(dummySlack, bestColMinv) {
					delta := dummySlack
					for j := 0; j <= activeR; j++ {
						if used[j] {
							u[p[j]] = addCost2(u[p[j]], delta)
							if j < activeR {
								v[j] = subCost2(v[j], delta)
							}
						}
					}
					if dummySrcCol != activeR {
						curCol := dummySrcCol
						for curCol != activeR {
							prev := way[curCol]
							p[curCol] = p[prev]
							curCol = prev
						}
					}
					break
				}

				delta := bestColMinv
				for j := 0; j <= activeR; j++ {
					if used[j] {
						u[p[j]] = addCost2(u[p[j]], delta)
						if j < activeR {
							v[j] = subCost2(v[j], delta)
						}
					} else if j < activeR && hasMinv[j] {
						minv[j] = subCost2(minv[j], delta)
					}
				}
				dummySlack = subCost2(dummySlack, delta)
				j0 = j1
				if p[j0] < 0 {
					curCol := j0
					for curCol != activeR {
						prev := way[curCol]
						p[curCol] = p[prev]
						curCol = prev
					}
					break
				}
				candDummy := subCost2(skipCost, u[p[j0]])
				if lessCost2(candDummy, dummySlack) {
					dummySlack = candDummy
					dummySrcCol = j0
				}
			}
		}
	} else {
		skipCost := cost3{miss: 1}
		costs := make([]cost3, activeL*activeR)
		u := make([]cost3, activeL)
		for i := range u {
			u[i] = skipCost
		}
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			exists[al*activeR+ar] = true
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			c := cost3{negW: -e.weight, negT: -tie}
			costs[al*activeR+ar] = c
			if lessCost3(c, u[al]) {
				u[al] = c
			}
		}
		v := make([]cost3, activeR)

		for i := 0; i < activeL; i++ {
			base := i * activeR
			ui := u[i]
			for j := 0; j < activeR; j++ {
				if p[j] < 0 && exists[base+j] && costs[base+j] == ui {
					p[j] = i
					rowMatched[i] = true
					break
				}
			}
		}

		minv := make([]cost3, activeR)
		for startRow := 0; startRow < activeL; startRow++ {
			if rowMatched[startRow] {
				continue
			}
			p[activeR] = startRow
			j0 := activeR
			for j := 0; j <= activeR; j++ {
				used[j] = false
				if j < activeR {
					hasMinv[j] = false
				}
				way[j] = activeR
			}
			dummySlack := subCost3(skipCost, u[startRow])
			dummySrcCol := activeR

			for {
				used[j0] = true
				i0 := p[j0]
				base := i0 * activeR
				ui0 := u[i0]
				var bestColMinv cost3
				j1 := -1
				for j := 0; j < activeR; j++ {
					if used[j] {
						continue
					}
					if exists[base+j] {
						cur := subCost3(costs[base+j], addCost3(ui0, v[j]))
						if !hasMinv[j] || lessCost3(cur, minv[j]) {
							minv[j] = cur
							hasMinv[j] = true
							way[j] = j0
						}
					}
					if hasMinv[j] && (j1 < 0 || lessCost3(minv[j], bestColMinv)) {
						bestColMinv = minv[j]
						j1 = j
					}
				}
				if j1 < 0 || lessCost3(dummySlack, bestColMinv) {
					delta := dummySlack
					for j := 0; j <= activeR; j++ {
						if used[j] {
							u[p[j]] = addCost3(u[p[j]], delta)
							if j < activeR {
								v[j] = subCost3(v[j], delta)
							}
						}
					}
					if dummySrcCol != activeR {
						curCol := dummySrcCol
						for curCol != activeR {
							prev := way[curCol]
							p[curCol] = p[prev]
							curCol = prev
						}
					}
					break
				}

				delta := bestColMinv
				for j := 0; j <= activeR; j++ {
					if used[j] {
						u[p[j]] = addCost3(u[p[j]], delta)
						if j < activeR {
							v[j] = subCost3(v[j], delta)
						}
					} else if j < activeR && hasMinv[j] {
						minv[j] = subCost3(minv[j], delta)
					}
				}
				dummySlack = subCost3(dummySlack, delta)
				j0 = j1
				if p[j0] < 0 {
					curCol := j0
					for curCol != activeR {
						prev := way[curCol]
						p[curCol] = p[prev]
						curCol = prev
					}
					break
				}
				candDummy := subCost3(skipCost, u[p[j0]])
				if lessCost3(candDummy, dummySlack) {
					dummySlack = candDummy
					dummySrcCol = j0
				}
			}
		}
	}

	for j := 0; j < activeR; j++ {
		if p[j] >= 0 && p[j] < activeL {
			assign[origLeft[p[j]]] = origRight[j]
		}
	}
}

func solveSparseHungarian(
	activeL, activeR, origNumL, origNumR int,
	leftMap, rightMap, origLeft, origRight []int,
	packOK bool,
	tieScale int64,
	dedup []bipartiteEdge,
	assign []int,
) {
	rowHead := make([]int, activeL+1)
	for _, e := range dedup {
		rowHead[leftMap[e.left]+1]++
	}
	for i := 1; i <= activeL; i++ {
		rowHead[i] += rowHead[i-1]
	}
	edgeCol := make([]int, len(dedup))
	cur := make([]int, activeL)
	copy(cur, rowHead[:activeL])

	rowMatch := make([]int, activeL)
	for i := range rowMatch {
		rowMatch[i] = -1
	}
	colMatch := make([]int, activeR)
	for j := range colMatch {
		colMatch[j] = -1
	}

	colSeenGen := make([]int, activeR)
	colClosedGen := make([]int, activeR)
	parentRow := make([]int, activeR)
	heapPos := make([]int, activeR)
	for j := range heapPos {
		heapPos[j] = -1
	}
	var heapArr []int
	var closedRows []int
	var closedCols []int
	gen := 0

	if packOK {
		skipCost := cost2Pair{a: 1, b: 0}
		edgeCost := make([]cost2Pair, len(dedup))
		u := make([]cost2Pair, activeL)
		for i := range u {
			u[i] = skipCost
		}
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			idx := cur[al]
			cur[al] = idx + 1
			edgeCol[idx] = ar
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			c := cost2Pair{a: 0, b: -(e.weight*tieScale + tie)}
			edgeCost[idx] = c
			if lessCost2(c, u[al]) {
				u[al] = c
			}
		}
		v := make([]cost2Pair, activeR)
		rowDist := make([]cost2Pair, activeL)
		colDist := make([]cost2Pair, activeR)

		// Initial zero-rc greedy pass.
		for i := 0; i < activeL; i++ {
			ui := u[i]
			for ei := rowHead[i]; ei < rowHead[i+1]; ei++ {
				j := edgeCol[ei]
				if colMatch[j] < 0 && edgeCost[ei] == ui {
					rowMatch[i] = j
					colMatch[j] = i
					break
				}
			}
		}

		heapPushOrUpdate := func(col int) {
			idx := heapPos[col]
			if idx < 0 {
				idx = len(heapArr)
				heapArr = append(heapArr, col)
				heapPos[col] = idx
			}
			for idx > 0 {
				p := (idx - 1) / 2
				pc := heapArr[p]
				if !lessCost2(colDist[col], colDist[pc]) {
					break
				}
				heapArr[idx] = pc
				heapPos[pc] = idx
				idx = p
			}
			heapArr[idx] = col
			heapPos[col] = idx
		}
		heapPop := func() int {
			top := heapArr[0]
			heapPos[top] = -1
			n := len(heapArr) - 1
			if n > 0 {
				last := heapArr[n]
				heapArr[0] = last
				heapPos[last] = 0
			}
			heapArr = heapArr[:n]
			i := 0
			for {
				l := 2*i + 1
				if l >= n {
					break
				}
				best := l
				r := l + 1
				if r < n && lessCost2(colDist[heapArr[r]], colDist[heapArr[l]]) {
					best = r
				}
				if !lessCost2(colDist[heapArr[best]], colDist[heapArr[i]]) {
					break
				}
				heapArr[i], heapArr[best] = heapArr[best], heapArr[i]
				heapPos[heapArr[i]] = i
				heapPos[heapArr[best]] = best
				i = best
			}
			return top
		}

		for startRow := 0; startRow < activeL; startRow++ {
			if rowMatch[startRow] >= 0 {
				continue
			}
			gen++
			heapArr = heapArr[:0]
			closedRows = closedRows[:0]
			closedCols = closedCols[:0]

			rowDist[startRow] = cost2Pair{}
			closedRows = append(closedRows, startRow)
			bestDummyDist := subCost2(skipCost, u[startRow])
			bestDummyRow := startRow

			for ei := rowHead[startRow]; ei < rowHead[startRow+1]; ei++ {
				j := edgeCol[ei]
				cand := subCost2(edgeCost[ei], addCost2(u[startRow], v[j]))
				if colSeenGen[j] != gen || lessCost2(cand, colDist[j]) {
					if colSeenGen[j] != gen {
						colSeenGen[j] = gen
						heapPos[j] = -1
					}
					colDist[j] = cand
					parentRow[j] = startRow
					heapPushOrUpdate(j)
				}
			}

			augTargetCol := -1
			augDist := bestDummyDist
			for len(heapArr) > 0 {
				j := heapPop()
				d := colDist[j]
				if !lessCost2(d, bestDummyDist) {
					break
				}
				colClosedGen[j] = gen
				closedCols = append(closedCols, j)
				if colMatch[j] < 0 {
					augTargetCol = j
					augDist = d
					break
				}
				i0 := colMatch[j]
				rowDist[i0] = d
				closedRows = append(closedRows, i0)
				candDummy := addCost2(d, subCost2(skipCost, u[i0]))
				if lessCost2(candDummy, bestDummyDist) {
					bestDummyDist = candDummy
					bestDummyRow = i0
				}
				ui0 := u[i0]
				for ei := rowHead[i0]; ei < rowHead[i0+1]; ei++ {
					nj := edgeCol[ei]
					if colClosedGen[nj] == gen {
						continue
					}
					rc := subCost2(edgeCost[ei], addCost2(ui0, v[nj]))
					cand := addCost2(d, rc)
					if colSeenGen[nj] != gen || lessCost2(cand, colDist[nj]) {
						if colSeenGen[nj] != gen {
							colSeenGen[nj] = gen
							heapPos[nj] = -1
						}
						colDist[nj] = cand
						parentRow[nj] = i0
						heapPushOrUpdate(nj)
					}
				}
			}

			if augTargetCol < 0 {
				augDist = bestDummyDist
			}

			// O(|closed|) dual update.
			for _, r := range closedRows {
				u[r] = addCost2(u[r], subCost2(augDist, rowDist[r]))
			}
			for _, c := range closedCols {
				v[c] = subCost2(v[c], subCost2(augDist, colDist[c]))
			}

			if augTargetCol >= 0 {
				c := augTargetCol
				for {
					r := parentRow[c]
					prevC := rowMatch[r]
					rowMatch[r] = c
					colMatch[c] = r
					if r == startRow {
						break
					}
					c = prevC
				}
			} else if bestDummyRow != startRow {
				c := rowMatch[bestDummyRow]
				rowMatch[bestDummyRow] = -1
				for {
					r := parentRow[c]
					prevC := rowMatch[r]
					rowMatch[r] = c
					colMatch[c] = r
					if r == startRow {
						break
					}
					c = prevC
				}
			}
		}
	} else {
		skipCost := cost3{miss: 1}
		edgeCost := make([]cost3, len(dedup))
		u := make([]cost3, activeL)
		for i := range u {
			u[i] = skipCost
		}
		for _, e := range dedup {
			al := leftMap[e.left]
			ar := rightMap[e.right]
			idx := cur[al]
			cur[al] = idx + 1
			edgeCol[idx] = ar
			tie := int64(origNumL-e.left) * int64(origNumR-origRight[ar])
			c := cost3{negW: -e.weight, negT: -tie}
			edgeCost[idx] = c
			if lessCost3(c, u[al]) {
				u[al] = c
			}
		}
		v := make([]cost3, activeR)
		rowDist := make([]cost3, activeL)
		colDist := make([]cost3, activeR)

		for i := 0; i < activeL; i++ {
			ui := u[i]
			for ei := rowHead[i]; ei < rowHead[i+1]; ei++ {
				j := edgeCol[ei]
				if colMatch[j] < 0 && edgeCost[ei] == ui {
					rowMatch[i] = j
					colMatch[j] = i
					break
				}
			}
		}

		heapPushOrUpdate := func(col int) {
			idx := heapPos[col]
			if idx < 0 {
				idx = len(heapArr)
				heapArr = append(heapArr, col)
				heapPos[col] = idx
			}
			for idx > 0 {
				p := (idx - 1) / 2
				pc := heapArr[p]
				if !lessCost3(colDist[col], colDist[pc]) {
					break
				}
				heapArr[idx] = pc
				heapPos[pc] = idx
				idx = p
			}
			heapArr[idx] = col
			heapPos[col] = idx
		}
		heapPop := func() int {
			top := heapArr[0]
			heapPos[top] = -1
			n := len(heapArr) - 1
			if n > 0 {
				last := heapArr[n]
				heapArr[0] = last
				heapPos[last] = 0
			}
			heapArr = heapArr[:n]
			i := 0
			for {
				l := 2*i + 1
				if l >= n {
					break
				}
				best := l
				r := l + 1
				if r < n && lessCost3(colDist[heapArr[r]], colDist[heapArr[l]]) {
					best = r
				}
				if !lessCost3(colDist[heapArr[best]], colDist[heapArr[i]]) {
					break
				}
				heapArr[i], heapArr[best] = heapArr[best], heapArr[i]
				heapPos[heapArr[i]] = i
				heapPos[heapArr[best]] = best
				i = best
			}
			return top
		}

		for startRow := 0; startRow < activeL; startRow++ {
			if rowMatch[startRow] >= 0 {
				continue
			}
			gen++
			heapArr = heapArr[:0]
			closedRows = closedRows[:0]
			closedCols = closedCols[:0]

			rowDist[startRow] = cost3{}
			closedRows = append(closedRows, startRow)
			bestDummyDist := subCost3(skipCost, u[startRow])
			bestDummyRow := startRow

			for ei := rowHead[startRow]; ei < rowHead[startRow+1]; ei++ {
				j := edgeCol[ei]
				cand := subCost3(edgeCost[ei], addCost3(u[startRow], v[j]))
				if colSeenGen[j] != gen || lessCost3(cand, colDist[j]) {
					if colSeenGen[j] != gen {
						colSeenGen[j] = gen
						heapPos[j] = -1
					}
					colDist[j] = cand
					parentRow[j] = startRow
					heapPushOrUpdate(j)
				}
			}

			augTargetCol := -1
			augDist := bestDummyDist
			for len(heapArr) > 0 {
				j := heapPop()
				d := colDist[j]
				if !lessCost3(d, bestDummyDist) {
					break
				}
				colClosedGen[j] = gen
				closedCols = append(closedCols, j)
				if colMatch[j] < 0 {
					augTargetCol = j
					augDist = d
					break
				}
				i0 := colMatch[j]
				rowDist[i0] = d
				closedRows = append(closedRows, i0)
				candDummy := addCost3(d, subCost3(skipCost, u[i0]))
				if lessCost3(candDummy, bestDummyDist) {
					bestDummyDist = candDummy
					bestDummyRow = i0
				}
				ui0 := u[i0]
				for ei := rowHead[i0]; ei < rowHead[i0+1]; ei++ {
					nj := edgeCol[ei]
					if colClosedGen[nj] == gen {
						continue
					}
					rc := subCost3(edgeCost[ei], addCost3(ui0, v[nj]))
					cand := addCost3(d, rc)
					if colSeenGen[nj] != gen || lessCost3(cand, colDist[nj]) {
						if colSeenGen[nj] != gen {
							colSeenGen[nj] = gen
							heapPos[nj] = -1
						}
						colDist[nj] = cand
						parentRow[nj] = i0
						heapPushOrUpdate(nj)
					}
				}
			}

			if augTargetCol < 0 {
				augDist = bestDummyDist
			}

			for _, r := range closedRows {
				u[r] = addCost3(u[r], subCost3(augDist, rowDist[r]))
			}
			for _, c := range closedCols {
				v[c] = subCost3(v[c], subCost3(augDist, colDist[c]))
			}

			if augTargetCol >= 0 {
				c := augTargetCol
				for {
					r := parentRow[c]
					prevC := rowMatch[r]
					rowMatch[r] = c
					colMatch[c] = r
					if r == startRow {
						break
					}
					c = prevC
				}
			} else if bestDummyRow != startRow {
				c := rowMatch[bestDummyRow]
				rowMatch[bestDummyRow] = -1
				for {
					r := parentRow[c]
					prevC := rowMatch[r]
					rowMatch[r] = c
					colMatch[c] = r
					if r == startRow {
						break
					}
					c = prevC
				}
			}
		}
	}

	for i := 0; i < activeL; i++ {
		if c := rowMatch[i]; c >= 0 {
			assign[origLeft[i]] = origRight[c]
		}
	}
}

func solveFastCSRSSAP(
	numLeft, numRight int,
	rightCap []int,
	leftGroup []int,
	midKeys []groupRightKey,
	dedup []bipartiteEdge,
	assign []int,
) {
	var midNode map[groupRightKey]int
	if len(midKeys) > 0 {
		midNode = make(map[groupRightKey]int, len(midKeys))
		for idx, k := range midKeys {
			midNode[k] = 1 + numLeft + idx
		}
	}

	numMid := len(midKeys)
	rightBase := 1 + numLeft + numMid
	sink := rightBase + numRight
	numNodes := sink + 1
	const source = 0

	// Count forward edges:
	// source->left (numLeft), left->mid/right (len(dedup)), mid->right (numMid), right->sink (<=numRight).
	numRightCapPos := 0
	for r := 0; r < numRight; r++ {
		capVal := 1
		if rightCap != nil {
			if r < len(rightCap) {
				capVal = rightCap[r]
			} else {
				capVal = 0
			}
		}
		if capVal > 0 {
			numRightCapPos++
		}
	}
	numFwd := numLeft + len(dedup) + numMid + numRightCapPos
	fwdU := make([]int, 0, numFwd)
	fwdV := make([]int, 0, numFwd)
	fwdCap := make([]int, 0, numFwd)
	fwdCostW := make([]int64, 0, numFwd)
	fwdCostT := make([]int64, 0, numFwd)

	deg := make([]int, numNodes)
	addFwd := func(u, v, capVal int, cw, ct int64) {
		fwdU = append(fwdU, u)
		fwdV = append(fwdV, v)
		fwdCap = append(fwdCap, capVal)
		fwdCostW = append(fwdCostW, cw)
		fwdCostT = append(fwdCostT, ct)
		deg[u]++
		deg[v]++
	}

	for i := 0; i < numLeft; i++ {
		addFwd(source, 1+i, 1, 0, 0)
	}
	midToRightAdded := make([]bool, numMid)
	for _, e := range dedup {
		tie := int64(numLeft-e.left) * int64(numRight-e.right)
		if midNode != nil {
			if mIdx, ok := midNode[groupRightKey{group: leftGroup[e.left], right: e.right}]; ok {
				addFwd(1+e.left, mIdx, 1, -e.weight, -tie)
				midOffset := mIdx - (1 + numLeft)
				if !midToRightAdded[midOffset] {
					midToRightAdded[midOffset] = true
					addFwd(mIdx, rightBase+e.right, 1, 0, 0)
				}
				continue
			}
		}
		addFwd(1+e.left, rightBase+e.right, 1, -e.weight, -tie)
	}
	for r := 0; r < numRight; r++ {
		capVal := 1
		if rightCap != nil {
			if r < len(rightCap) {
				capVal = rightCap[r]
			} else {
				capVal = 0
			}
		}
		if capVal > 0 {
			addFwd(rightBase+r, sink, capVal, 0, 0)
		}
	}

	// Build flat contiguous SoA CSR arrays.
	csrHead := make([]int, numNodes+1)
	for u := 0; u < numNodes; u++ {
		csrHead[u+1] = csrHead[u] + deg[u]
	}
	totalEdges := csrHead[numNodes]
	csrTo := make([]int, totalEdges)
	csrRev := make([]int, totalEdges)
	csrCap := make([]int, totalEdges)
	csrCostW := make([]int64, totalEdges)
	csrCostT := make([]int64, totalEdges)

	curPos := make([]int, numNodes)
	copy(curPos, csrHead[:numNodes])
	for k := range fwdU {
		u := fwdU[k]
		v := fwdV[k]
		fIdx := curPos[u]
		curPos[u] = fIdx + 1
		rIdx := curPos[v]
		curPos[v] = rIdx + 1

		cw, ct := fwdCostW[k], fwdCostT[k]
		csrTo[fIdx] = v
		csrRev[fIdx] = rIdx
		csrCap[fIdx] = fwdCap[k]
		csrCostW[fIdx] = cw
		csrCostT[fIdx] = ct

		csrTo[rIdx] = u
		csrRev[rIdx] = fIdx
		csrCap[rIdx] = 0
		csrCostW[rIdx] = -cw
		csrCostT[rIdx] = -ct
	}

	// DAG topological DP initialization for Johnson potentials.
	potW := make([]int64, numNodes)
	potT := make([]int64, numNodes)
	potReached := make([]bool, numNodes)
	potReached[source] = true
	for i := 0; i < numLeft; i++ {
		potReached[1+i] = true
	}
	relaxLayer := func(start, end int) {
		for u := start; u < end; u++ {
			if !potReached[u] {
				continue
			}
			puW, puT := potW[u], potT[u]
			for ei := csrHead[u]; ei < csrHead[u+1]; ei++ {
				if csrCap[ei] <= 0 {
					continue
				}
				v := csrTo[ei]
				cw := puW + csrCostW[ei]
				ct := puT + csrCostT[ei]
				if !potReached[v] || cw < potW[v] || (cw == potW[v] && ct < potT[v]) {
					potW[v] = cw
					potT[v] = ct
					potReached[v] = true
				}
			}
		}
	}
	relaxLayer(1, 1+numLeft)
	if numMid > 0 {
		relaxLayer(1+numLeft, rightBase)
	}
	relaxLayer(rightBase, sink)

	// Successive Shortest Augmenting Path with generation stamps & 8B indexed min-heap.
	distW := make([]int64, numNodes)
	distT := make([]int64, numNodes)
	seenGen := make([]int, numNodes)
	closedGen := make([]int, numNodes)
	parentNode := make([]int, numNodes)
	parentEdge := make([]int, numNodes)
	heapPos := make([]int, numNodes)
	for i := range heapPos {
		heapPos[i] = -1
	}
	var heapNodes []int
	var closedList []int
	gen := 0

	lessNodeDist := func(a, b int) bool {
		if distW[a] != distW[b] {
			return distW[a] < distW[b]
		}
		return distT[a] < distT[b]
	}
	heapPushOrUpdate := func(v int) {
		idx := heapPos[v]
		if idx < 0 {
			idx = len(heapNodes)
			heapNodes = append(heapNodes, v)
			heapPos[v] = idx
		}
		for idx > 0 {
			p := (idx - 1) / 2
			pv := heapNodes[p]
			if !lessNodeDist(v, pv) {
				break
			}
			heapNodes[idx] = pv
			heapPos[pv] = idx
			idx = p
		}
		heapNodes[idx] = v
		heapPos[v] = idx
	}
	heapPop := func() int {
		top := heapNodes[0]
		heapPos[top] = -1
		n := len(heapNodes) - 1
		if n > 0 {
			last := heapNodes[n]
			heapNodes[0] = last
			heapPos[last] = 0
		}
		heapNodes = heapNodes[:n]
		i := 0
		for {
			l := 2*i + 1
			if l >= n {
				break
			}
			best := l
			r := l + 1
			if r < n && lessNodeDist(heapNodes[r], heapNodes[l]) {
				best = r
			}
			if !lessNodeDist(heapNodes[best], heapNodes[i]) {
				break
			}
			heapNodes[i], heapNodes[best] = heapNodes[best], heapNodes[i]
			heapPos[heapNodes[i]] = i
			heapPos[heapNodes[best]] = best
			i = best
		}
		return top
	}

	for {
		gen++
		heapNodes = heapNodes[:0]
		closedList = closedList[:0]
		seenGen[source] = gen
		heapPos[source] = -1
		distW[source] = 0
		distT[source] = 0
		heapPushOrUpdate(source)

		for len(heapNodes) > 0 {
			u := heapPop()
			closedGen[u] = gen
			closedList = append(closedList, u)
			if u == sink {
				break
			}
			duW, duT := distW[u], distT[u]
			puW, puT := potW[u], potT[u]
			for ei := csrHead[u]; ei < csrHead[u+1]; ei++ {
				if csrCap[ei] <= 0 {
					continue
				}
				v := csrTo[ei]
				if closedGen[v] == gen {
					continue
				}
				candW := duW + csrCostW[ei] + puW - potW[v]
				candT := duT + csrCostT[ei] + puT - potT[v]
				if seenGen[v] != gen || candW < distW[v] || (candW == distW[v] && candT < distT[v]) {
					if seenGen[v] != gen {
						seenGen[v] = gen
						heapPos[v] = -1
					}
					distW[v] = candW
					distT[v] = candT
					parentNode[v] = u
					parentEdge[v] = ei
					heapPushOrUpdate(v)
				}
			}
		}

		if closedGen[sink] != gen {
			break
		}

		sinkDistW, sinkDistT := distW[sink], distT[sink]
		for _, v := range closedList {
			potW[v] += distW[v] - sinkDistW
			potT[v] += distT[v] - sinkDistT
		}

		for v := sink; v != source; v = parentNode[v] {
			ei := parentEdge[v]
			rev := csrRev[ei]
			csrCap[ei]--
			csrCap[rev]++
		}
	}

	// Extract assignment from saturated forward edges leaving each left node.
	for i := 0; i < numLeft; i++ {
		u := 1 + i
		for ei := csrHead[u]; ei < csrHead[u+1]; ei++ {
			if csrCap[ei] != 0 {
				continue
			}
			to := csrTo[ei]
			if to >= rightBase && to < sink {
				assign[i] = to - rightBase
				break
			}
			if to >= 1+numLeft && to < rightBase {
				for mei := csrHead[to]; mei < csrHead[to+1]; mei++ {
					if csrCap[mei] == 0 && csrTo[mei] >= rightBase && csrTo[mei] < sink {
						assign[i] = csrTo[mei] - rightBase
						break
					}
				}
				if assign[i] >= 0 {
					break
				}
			}
		}
	}
}
