package rate_limit

import (
	"math"
	"sync"
	"time"
)

/*
秒分甚至更大级别的流控，脉冲类型的流量抵抗力很弱，有极大潜在风险压垮系统
毫秒级别流控,脉冲流量很大可能造成有损(会拒绝很多流量)
如果既想控制流量曲线，又想无损，一般做法是通过匀速排队的控制策略，平滑掉流量。
*/

type Limiter struct {
	// statIntervalInMs 表示一个时间间隔
	statIntervalInMs time.Duration
	// threshold 在一个间隔下的临界值
	threshold int64
	// 是否使用均匀流控
	limitEvenly bool
	// 最长排队等待时间,和limitEvenly一起使用
	maxQueueTimeout time.Duration

	mu sync.Mutex

	currWindow Window
	prevWindow Window
}

func NewLimiter(statIntervalInMs time.Duration, threshold int64, currWindow Window) *Limiter {
	return &Limiter{
		statIntervalInMs: statIntervalInMs,
		threshold:        threshold,
		currWindow:       currWindow,
		prevWindow:       currWindow,
	}
}

func (lim *Limiter) WithLimitEvenly(limitEvenly bool) *Limiter {
	lim.limitEvenly = limitEvenly
	return lim
}

func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

func (lim *Limiter) AllowN(now time.Time, n int64) bool {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	if lim.limitEvenly && now.Sub(lim.currWindow.LastTime()).Milliseconds() < lim.statIntervalInMs.Milliseconds()/lim.threshold {
		return false
	}

	newCurrentStart := now.Truncate(lim.statIntervalInMs)
	timeSpan := newCurrentStart.Sub(lim.currWindow.StartTime()) / lim.statIntervalInMs
	if timeSpan >= 1 {
		newPrevCount := int64(0)
		if timeSpan == 1 {
			newPrevCount = lim.currWindow.Count()
		}
		lim.prevWindow.Reset(newCurrentStart.Add(-lim.statIntervalInMs), newPrevCount)

		lim.currWindow.Reset(newCurrentStart, 0)
	}

	elapsed := now.Sub(lim.currWindow.StartTime())

	weight := float64(lim.statIntervalInMs-elapsed) / float64(lim.statIntervalInMs)
	count := int64(math.Floor(weight*float64(lim.prevWindow.Count()))) + lim.currWindow.Count()
	// 进行同步
	defer lim.currWindow.Sync(now)

	if count+n > lim.threshold {
		return false
	}

	lim.currWindow.AddCount(n)
	return true
}
