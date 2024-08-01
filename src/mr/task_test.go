package mr

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func echo(mngr BatchTaskManager) {
	for {
		task := mngr.Take(1)
		if task == nil {
			return
		}
		mngr.ReportSuccess(task.TaskHeader, task.Desc)
	}
}

func fail(mngr BatchTaskManager) {
	for {
		task := mngr.Take(1)
		if task == nil {
			return
		}
		mngr.ReportFailure(task.TaskHeader)
	}
}

func delay(mngr BatchTaskManager) {
	for {
		task := mngr.Take(1)
		if task == nil {
			return
		}
		time.Sleep(time.Second)
		mngr.ReportSuccess(task.TaskHeader, task.Desc)
	}
}

func TestTaskManager_OneBatch(t *testing.T) {
	rq := require.New(t)
	var wg sync.WaitGroup

	var mngr BatchTaskManager
	mngr = NewInMemoryManager(100 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		echo(mngr)
	}()

	runBatch(rq, []interface{}{
		"1-1", "1-2", "1-3", "1-4", "1-5",
	}, mngr)

	mngr.Close()
	wg.Wait()
}

func TestTaskManager_FailTask(t *testing.T) {
	rq := require.New(t)
	var wg sync.WaitGroup

	var mngr BatchTaskManager
	mngr = NewInMemoryManager(3 * time.Second)

	wg.Add(2)
	go func() {
		defer wg.Done()
		echo(mngr)
	}()

	go func() {
		defer wg.Done()
		fail(mngr)
	}()

	runBatch(rq, []interface{}{
		"1-1", "1-2", "1-3", "1-4", "1-5",
	}, mngr)

	mngr.Close()
	wg.Wait()
}

func TestTaskManager_Timeout(t *testing.T) {
	rq := require.New(t)
	var wg sync.WaitGroup

	var mngr BatchTaskManager
	mngr = NewInMemoryManager(time.Millisecond)

	wg.Add(3)
	go func() {
		defer wg.Done()
		echo(mngr)
	}()

	go func() {
		defer wg.Done()
		delay(mngr)
	}()

	go func() {
		defer wg.Done()
		delay(mngr)
	}()

	runBatch(rq, []interface{}{
		"1-1", "1-2", "1-3", "1-4", "1-5",
	}, mngr)

	mngr.Close()
	wg.Wait()
}

func TestTaskManager_TwoBatch(t *testing.T) {
	rq := require.New(t)
	var wg sync.WaitGroup

	var mngr BatchTaskManager
	mngr = NewInMemoryManager(100 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		echo(mngr)
	}()

	runBatch(rq, []interface{}{
		"1-1", "1-2", "1-3", "1-4", "1-5",
	}, mngr)

	runBatch(rq, []interface{}{
		"2-1", "2-2", "2-3", "2-4",
	}, mngr)

	mngr.Close()
	wg.Wait()
}

func runBatch(rq *require.Assertions, input []interface{}, mngr BatchTaskManager) {
	batch, err := mngr.Put(input)
	rq.NoError(err)

	output := batch.Wait()
	rq.Equal(len(input), len(output))
}
