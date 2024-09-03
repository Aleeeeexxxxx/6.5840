package shardctrler

import "sort"

type ConfigBalancer struct {
	cfg *Config

	assignments map[int][]int // gid -> shards
	unassigned  []int
}

func NewConfigBalancer(cfg *Config) *ConfigBalancer {
	cb := &ConfigBalancer{
		cfg:         cfg,
		assignments: map[int][]int{},
	}
	return cb
}

func (cb *ConfigBalancer) parseAssignments() {
	for gid := range cb.cfg.Groups {
		cb.assignments[gid] = []int{}
	}

	for shard, gid := range cb.cfg.Shards {
		if gid == unassign {
			cb.unassigned = append(cb.unassigned, shard)
		} else {
			cb.assignments[gid] = append(cb.assignments[gid], shard)
		}
	}
}

func (cb *ConfigBalancer) max() ([]int, int) {
	max := -1
	var maxGids []int

	for gid, shards := range cb.assignments {
		if len(shards) > max {
			max = len(shards)
			maxGids = []int{gid}
		} else if len(shards) == max {
			maxGids = append(maxGids, gid)
		}
	}

	sort.Slice(maxGids, func(i, j int) bool {
		return maxGids[i] < maxGids[j]
	})

	return maxGids, max
}

func (cb *ConfigBalancer) min() ([]int, int) {
	min := -1
	var minGids []int

	for gid, shards := range cb.assignments {
		if min == -1 || len(shards) < min {
			min = len(shards)
			minGids = []int{gid}
		} else if len(shards) == min {
			minGids = append(minGids, gid)
		}
	}

	sort.Slice(minGids, func(i, j int) bool {
		return minGids[i] < minGids[j]
	})

	return minGids, min
}

func (cb *ConfigBalancer) assignUnassigned() {
	if len(cb.assignments) == 0 {
		return
	}

	for len(cb.unassigned) > 0 {
		minGids, _ := cb.min()

		for _, gid := range minGids {
			if len(cb.unassigned) == 0 {
				break
			}

			shard := cb.unassigned[0]
			cb.unassigned = cb.unassigned[1:]

			cb.assignments[gid] = append(cb.assignments[gid], shard)
		}

	}
}

func (cb *ConfigBalancer) rebalanceAssignments() {
	if len(cb.assignments) == 0 {
		return
	}

	for {
		maxGids, max := cb.max()
		minGids, min := cb.min()

		if max-min <= 1 {
			break
		}

		for len(maxGids) > 0 && len(minGids) > 0 {
			maxGid := maxGids[0]
			minGid := minGids[0]
			shard := cb.assignments[maxGid][0]

			cb.assignments[maxGid] = cb.assignments[maxGid][1:]
			cb.assignments[minGid] = append(cb.assignments[minGid], shard)

			maxGids = maxGids[1:]
			minGids = minGids[1:]
		}
	}

}

func (cb *ConfigBalancer) Balance() {
	cb.parseAssignments()

	cb.assignUnassigned()
	cb.rebalanceAssignments()

	for gid, shards := range cb.assignments {
		for _, shard := range shards {
			cb.cfg.Shards[shard] = gid
		}
	}
}
