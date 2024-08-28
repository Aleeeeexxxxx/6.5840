package kvraft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStorage_Serialize(t *testing.T) {
	rq := require.New(t)
	data := NewDataStorage(0)

	expectedData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	expectedLastAppliedIndex := 2

	data.lastAppliedIndex = expectedLastAppliedIndex
	data.data = expectedData

	raw, serializedLastAppliedIndex := data.Serialize()
	rq.Equal(serializedLastAppliedIndex, 2)

	data.data = nil
	data.lastAppliedIndex = -1

	data.Deserialize(serializedLastAppliedIndex, raw)
	rq.EqualValues(expectedData, data.data)
	rq.Equal(expectedLastAppliedIndex, data.lastAppliedIndex)
}

func TestClerkStorage_Serialize(t *testing.T) {
	rq := require.New(t)
	cm := NewClerkStorage(0)

	expectedData := map[int32]*Client{
		1: {MessageID: 1, Value: "value1"},
		2: {MessageID: 1, Value: "value1"},
	}

	cm.data = expectedData
	raw := cm.Serialize()
	// expectedSerialized := string(raw)
	// fmt.Println(expectedSerialized)

	cm.data = nil

	cm.Deserialize(raw)
	rq.EqualValues(expectedData, cm.data)
}
