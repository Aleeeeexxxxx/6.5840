package shardctrler

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

func (cb *ConfigBalancer) assignUnassigned() {
	if len(cb.assignments) == 0 {
		return
	}

	for len(cb.unassigned) > 0 {
		min := -1
		minGids := []int{}

		for gid, shards := range cb.assignments {
			if min == -1 || len(shards) < min {
				min = len(shards)
				minGids = []int{gid}
			} else if len(shards) == min {
				minGids = append(minGids, gid)
			}
		}

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
		var maxGid, minGid int
		max := -1
		min := -1

		for gid, shards := range cb.assignments {
			if len(shards) > max {
				max = len(shards)
				maxGid = gid
			}
			if min == -1 || len(shards) < min {
				min = len(shards)
				minGid = gid
			}
		}

		if max-min <= 1 {
			return
		}

		move := cb.assignments[maxGid][len(cb.assignments[maxGid])-1]

		cb.assignments[maxGid] = cb.assignments[maxGid][:len(cb.assignments[maxGid])-1]
		cb.assignments[minGid] = append(cb.assignments[minGid], move)
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
