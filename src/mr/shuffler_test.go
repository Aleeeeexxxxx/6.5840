package mr

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testFolder = "tests"

	kvFile = "test-kv.json"
)

func Test_JsonStore(t *testing.T) {
	rq := require.New(t)
	store := NewJsonStore(filepath.Join(testFolder, "store"))

	t.Run("kv", func(t *testing.T) {
		expected := []KeyValue{
			{
				Key:   "1",
				Value: "1",
			},
			{
				Key:   "2",
				Value: "2",
			},
		}
		rq.NoError(store.StoreKV(kvFile, expected))

		actual, err := store.RetrieveKVBatch(kvFile)
		rq.NoError(err)
		rq.EqualValues(expected, actual)
	})
}

func Test_Shuffler(t *testing.T) {
	rq := require.New(t)

	shuffler, err := NewInMemoryShuffler("id", filepath.Join(testFolder, "shuffler"))
	rq.NoError(err)

	outputs, err := shuffler.Shuffle([]string{
		filepath.Join("test-shuffler-1.json"),
		filepath.Join("test-shuffler-2.json"),
		filepath.Join("test-shuffler-3.json"),
	})
	rq.NoError(err)
	rq.Equal(1, len(outputs))
}
