package rate_limit

import (
	"fmt"
	"testing"
	"time"
)

func TestSync(t *testing.T) {
	var (
		statIntervalInMs       = 1 * time.Second
		syncIntervalInMs       = 200 * time.Millisecond
		threshold        int64 = 10
	)
	// dataStore := NewRedisDataStore("localhost",6379, "", 0, statIntervalInMs)
	memoryDataStore := NewMemoryDataStore(statIntervalInMs)
	window := NewSyncWindow("test").WithSynchronizer(memoryDataStore, syncIntervalInMs)
	limit := NewLimiter(statIntervalInMs, threshold, window).WithLimitEvenly(true)
	for i := 0; i < 100000; i++ {
		if limit.Allow() {
			fmt.Printf("time:%d\n", time.Now().UnixNano()/1e3)
		}
		time.Sleep(10 * time.Microsecond)
	}
}
