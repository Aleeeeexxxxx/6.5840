package shardctrler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func ConfigShardsEqualOneOf(t *testing.T, cfg *Config, expected [][]int) {
	shardsEqualArray := func(array []int) bool {
		if len(array) != len(cfg.Shards) {
			return false
		}
		for i, item := range array {
			if cfg.Shards[i] != item {
				return false
			}
		}
		return true
	}

	for _, array := range expected {
		if shardsEqualArray(array) {
			return
		}
	}

	t.Fatalf("Shards not equal to any of the expected arrays")
}

func TestCfgMngr_Balance(t *testing.T) {
	rq := require.New(t)

	t.Run("empty", func(t *testing.T) {
		cfg := GetDefaultConfig()

		cb := NewConfigBalancer(cfg)
		cb.Balance()

		rq.Equal(0, len(cfg.Groups))
		for _, gid := range cfg.Shards {
			rq.Equal(unassign, gid)
		}
		rq.Equal(0, cfg.Num)
	})

	t.Run("one group, should assign all shards to this group", func(t *testing.T) {
		cfg := GetDefaultConfig()
		cfg.Groups[1] = []string{"a"}

		cb := NewConfigBalancer(cfg)
		cb.Balance()

		rq.Equal(1, len(cfg.Groups))
		for _, gid := range cfg.Shards {
			rq.Equal(1, gid)
		}
		rq.Equal(0, cfg.Num)
	})

	t.Run("several groups, mock 1 join", func(t *testing.T) {
		cfg := GetDefaultConfig()
		cfg.Groups[1] = []string{"a"}
		cfg.Groups[2] = []string{"a, b, c"}
		cfg.Shards = [10]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}

		cfg.Groups[3] = []string{"a"}

		cb := NewConfigBalancer(cfg)
		cb.Balance()

		rq.Equal(0, cfg.Num)
		rq.Equal(3, len(cfg.Groups))
		ConfigShardsEqualOneOf(t, cfg, [][]int{
			{1, 1, 1, 3, 3, 2, 2, 2, 2, 3},
			{1, 1, 1, 1, 3, 2, 2, 2, 3, 3},
		})
	})

	t.Run("several groups, mock 1 leave", func(t *testing.T) {
		cfg := GetDefaultConfig()
		cfg.Groups[1] = []string{"a"}
		cfg.Groups[2] = []string{"a, b, c"}
		cfg.Shards = [10]int{1, 1, 1, 1, 1, 2, 2, 2, unassign, unassign}

		cb := NewConfigBalancer(cfg)
		cb.Balance()

		rq.Equal(0, cfg.Num)
		rq.Equal(2, len(cfg.Groups))
		ConfigShardsEqualOneOf(t, cfg, [][]int{
			{1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
		})
	})

	t.Run("former 1 not assigned, has position now", func(t *testing.T) {
		cfg := GetDefaultConfig()
		cfg.Groups[1] = []string{"a"}
		cfg.Groups[2] = []string{"a, b, c"}
		cfg.Groups[3] = []string{"a"}
		cfg.Groups[4] = []string{"a, b, c"}
		cfg.Groups[5] = []string{"a"}
		cfg.Groups[6] = []string{"a, b, c"}
		cfg.Groups[7] = []string{"a"}
		cfg.Groups[8] = []string{"a, b, c"}
		cfg.Groups[9] = []string{"a"}
		cfg.Groups[10] = []string{"a, b, c"}
		cfg.Shards = [10]int{1, 2, 3, 4, 5, 6, 7, 8, 9, unassign}

		cb := NewConfigBalancer(cfg)
		cb.Balance()

		rq.Equal(0, cfg.Num)
		rq.Equal(10, len(cfg.Groups))
		ConfigShardsEqualOneOf(t, cfg, [][]int{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		})
	})

	t.Run("former 1 not assigned, no position now", func(t *testing.T) {
		cfg := GetDefaultConfig()
		cfg.Groups[1] = []string{"a"}
		cfg.Groups[2] = []string{"a, b, c"}
		cfg.Groups[3] = []string{"a"}
		cfg.Groups[4] = []string{"a, b, c"}
		cfg.Groups[5] = []string{"a"}
		cfg.Groups[6] = []string{"a, b, c"}
		cfg.Groups[7] = []string{"a"}
		cfg.Groups[8] = []string{"a, b, c"}
		cfg.Groups[9] = []string{"a"}
		cfg.Groups[10] = []string{"a, b, c"}
		cfg.Groups[11] = []string{"a, b, c"}
		cfg.Shards = [10]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 11}

		cb := NewConfigBalancer(cfg)
		cb.Balance()

		rq.Equal(0, cfg.Num)
		rq.Equal(11, len(cfg.Groups))
		ConfigShardsEqualOneOf(t, cfg, [][]int{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 11},
		})
	})
}
