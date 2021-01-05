package rate_limit

import (
	"time"
)

type SyncRequest struct {
	Key string
	// 开始时间戳
	Start int64
	// 数量
	Count int64
	// 期间内增量
	Delta int64
}

type SyncResponse struct {
	Success bool
	Start   int64

	// 本地的增量
	LocalDelta int64
	// 其他服务的增量
	CounterpartDelta int64
}

type SyncResponseHandler func(SyncResponse)
type GetSyncRequest func() SyncRequest

type Window interface {
	// 返回开始时间
	StartTime() time.Time

	// 返回累积count
	Count() int64

	// 添加n个count
	AddCount(n int64)

	// 重置一个window中的时间和count
	Reset(time.Time, int64)

	// 分布式的系统同步使用
	Sync(now time.Time)

	// 返回最新时间
	LastTime() time.Time
}

type SyncWindow struct {
	key       string
	startTime int64
	count     int64
	delta     int64
	lastTime  int64

	sync Synchronizer
}

func NewSyncWindow(key string) *SyncWindow {
	return &SyncWindow{
		key:       key,
		startTime: time.Now().Unix(),
		count:     0,
		delta:     0,
	}
}

func (s *SyncWindow) WithSynchronizer(store DataStore, syncIntervalInMs time.Duration) *SyncWindow {
	s.sync = newFastSynchronizer(store, syncIntervalInMs)
	s.sync.Start()
	return s
}

func (s *SyncWindow) StartTime() time.Time {
	return time.Unix(0, s.startTime)
}

func (s *SyncWindow) LastTime() time.Time {
	return time.Unix(0, s.lastTime)
}

func (s *SyncWindow) Count() int64 {
	return s.count
}

func (s *SyncWindow) AddCount(n int64) {
	s.delta += n
	s.count += n
	s.lastTime = time.Now().UnixNano()
}

func (s *SyncWindow) Reset(t time.Time, c int64) {
	s.startTime = t.UnixNano()
	s.count = c
}

func (s *SyncWindow) handleSyncResponse(res SyncResponse) {
	if res.Success && res.Start == s.startTime {
		s.count += res.CounterpartDelta
		s.delta -= res.LocalDelta
	}
}

func (s *SyncWindow) getSyncRequest() SyncRequest {
	return SyncRequest{
		Key:   s.key,
		Start: s.startTime,
		Count: s.count,
		Delta: s.delta,
	}
}

func (s *SyncWindow) Sync(now time.Time) {
	s.sync.Sync(now, s.getSyncRequest, s.handleSyncResponse)
}
