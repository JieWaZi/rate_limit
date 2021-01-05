package rate_limit

import (
	"go.uber.org/atomic"
	"log"
	"time"
)

type Synchronizer interface {
	Start()
	Stop()
	Sync(time.Time, GetSyncRequest, SyncResponseHandler)
}

type FastSynchronizer struct {
	reqChan  chan SyncRequest
	respChan chan SyncResponse
	stop     chan struct{}

	helper syncHelper
}

func newFastSynchronizer(store DataStore, syncIntervalInMs time.Duration) *FastSynchronizer {
	return &FastSynchronizer{
		reqChan:  make(chan SyncRequest),
		respChan: make(chan SyncResponse),
		stop:     make(chan struct{}),
		helper:   newSyncHelper(store, syncIntervalInMs),
	}
}

func (f *FastSynchronizer) Start() {
	go func() {
		for {
			select {
			case req := <-f.reqChan:
				resp, err := f.helper.sync(req)
				if err != nil {
					log.Printf("sync err:%s\n", err.Error())
				}
				f.respChan <- resp
			case <-f.stop:
				return
			default:
			}
		}
	}()
}
func (f *FastSynchronizer) Stop() {
	f.stop <- struct{}{}
}
func (f *FastSynchronizer) Sync(now time.Time, getRequest GetSyncRequest, handler SyncResponseHandler) {
	if f.helper.couldSync(now) {
		select {
		case f.reqChan <- getRequest():
			f.helper.start(now)
		default:
		}
	}

	if f.helper.running.Load() {
		select {
		case resp := <-f.respChan:
			handler(resp)
			f.helper.stop()
		default:
		}
	}
}

type syncHelper struct {
	store DataStore

	running      atomic.Bool
	syncInterval time.Duration
	lastSynced   time.Time
}

func newSyncHelper(store DataStore, syncIntervalInMs time.Duration) syncHelper {
	return syncHelper{
		store:        store,
		running:      atomic.Bool{},
		syncInterval: syncIntervalInMs,
		lastSynced:   time.Now(),
	}
}
func (s *syncHelper) couldSync(now time.Time) bool {
	return !s.running.Load() && now.Sub(s.lastSynced) >= s.syncInterval
}
func (s *syncHelper) start(now time.Time) {
	s.running.Store(true)
	s.lastSynced = now
}
func (s *syncHelper) stop() {
	s.running.Store(false)
}
func (s *syncHelper) sync(req SyncRequest) (SyncResponse, error) {
	var (
		newCount int64
		err      error
	)
	if req.Delta > 0 {
		newCount, err = s.store.Add(req.Key, req.Start, req.Delta)
	} else {
		newCount, err = s.store.Get(req.Key, req.Start)
	}
	if err != nil {
		if err == DataStoreKeyValueNotFoundError {
			return SyncResponse{}, nil
		}
		log.Printf("data store add/get key:%s, startTime:%d, err:%s", req.Key, req.Start, err.Error())
		return SyncResponse{}, err
	}

	return SyncResponse{
		Success:          true,
		Start:            req.Start,
		LocalDelta:       req.Delta,
		CounterpartDelta: newCount - req.Delta,
	}, nil
}
