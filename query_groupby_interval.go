package godruid

import (
	"time"
	"fmt"
	"strings"
)
// IntervalSlot query interval slot item
type IntervalSlot struct {
	TimePos time.Time `json:"timePos"`
	TimeLen int64     `json:"timeLen"`
}

func (i *IntervalSlot) ToInterval() string {
	endTime := i.TimePos.Add(time.Duration(i.TimeLen))
	return fmt.Sprintf("%v/%v", i.TimePos, endTime)
}

// DistributeQuery split intervals to whole days and hours
func (q *QueryGroupBy) distributeIntervalSlots() []IntervalSlot {
	ret := []IntervalSlot{}
	for _, i := range q.Intervals {
		for _, intervalSlot := range distributeIntervals(i) {
			ret = append(ret, intervalSlot)
		}
	}
	return ret
}

func distributeIntervals(interval string) []IntervalSlot {
	ret := []IntervalSlot{}
	intervalSlot, err := parseInterval(interval)
	if err == nil {
		// TODO: 按年，月，周
		dSi, dIs, dEi := distributeDays(*intervalSlot)
		if dSi != nil {
			hSi, hIs, hEi := distributeHours(*dSi)

			if hSi != nil {
				ret = append(ret, *hSi)
			}
			for _, hi := range hIs {
				ret = append(ret, hi)
			}
			if hEi != nil {
				ret = append(ret, *hEi)
			}
		}
		for _, di := range dIs {
			ret = append(ret, di)
		}
		if dEi != nil {
			hSi, hIs, hEi := distributeHours(*dEi)

			if hSi != nil {
				ret = append(ret, *hSi)
			}
			for _, hi := range hIs {
				ret = append(ret, hi)
			}
			if hEi != nil {
				ret = append(ret, *hEi)
			}
		}
	}
	return ret
}

func distributeDays(intervalSlot IntervalSlot) (*IntervalSlot, []IntervalSlot, *IntervalSlot) {
	var startIntervalSlot *IntervalSlot
	var endIntervalSlot *IntervalSlot
	daysTimeStarts := []IntervalSlot{}
	hour, min, sec := intervalSlot.TimePos.Clock()
	var timePosRel int64
	if hour > 0 || min > 0 || sec > 0 {
		startRestTimeLen := int64(24*3600 - hour*3600 - min*60 - sec)
		startIntervalSlot = &IntervalSlot{TimePos: intervalSlot.TimePos, TimeLen: startRestTimeLen}
		timePosRel += startRestTimeLen
	}

	for ; intervalSlot.TimeLen-timePosRel >= int64(86400); timePosRel += int64(86400) {
		daysTimeStarts = append(daysTimeStarts, IntervalSlot{
			TimePos: intervalSlot.TimePos.Add(time.Duration(timePosRel) * time.Second),
			TimeLen: int64(86400),
		})
	}
	if intervalSlot.TimeLen > timePosRel {
		t := intervalSlot.TimePos.Add(time.Duration(timePosRel) * time.Second)
		endRestTimeLen := intervalSlot.TimeLen - timePosRel
		endIntervalSlot = &IntervalSlot{TimePos: t, TimeLen: endRestTimeLen}
	}

	return startIntervalSlot, daysTimeStarts, endIntervalSlot
}

func distributeHours(intervalSlot IntervalSlot) (*IntervalSlot, []IntervalSlot, *IntervalSlot) {
	var startIntervalSlot *IntervalSlot
	var endIntervalSlot *IntervalSlot
	daysTimeStarts := []IntervalSlot{}
	_, min, sec := intervalSlot.TimePos.Clock()
	var timePosRel int64
	if min > 0 || sec > 0 {
		startRestTimeLen := int64(3600 - min*60 - sec)
		startIntervalSlot = &IntervalSlot{TimePos: intervalSlot.TimePos, TimeLen: startRestTimeLen}
		timePosRel += startRestTimeLen
	}

	for ; intervalSlot.TimeLen-timePosRel >= int64(3600); timePosRel += int64(3600) {
		daysTimeStarts = append(daysTimeStarts, IntervalSlot{
			TimePos: intervalSlot.TimePos.Add(time.Duration(timePosRel) * time.Second),
			TimeLen: int64(86400),
		})
	}
	if intervalSlot.TimeLen > timePosRel {
		t := intervalSlot.TimePos.Add(time.Duration(timePosRel) * time.Second)
		endRestTimeLen := intervalSlot.TimeLen - timePosRel
		endIntervalSlot = &IntervalSlot{TimePos: t, TimeLen: endRestTimeLen}
	}

	return startIntervalSlot, daysTimeStarts, endIntervalSlot
}

func parseInterval(interval string) (*IntervalSlot, error) {
	timeRange := strings.SplitN(interval, "/", 2)
	if len(timeRange) != 2 {
		return nil, fmt.Errorf("interval(%s) format is invalid", interval)
	}
	startTime, err1 := time.Parse(time.RFC3339, timeRange[0])
	if err1 != nil {
		return nil, err1
	}
	endTime, err2 := time.Parse(time.RFC3339, timeRange[1])
	if err2 != nil {
		return nil, err2
	}
	timeLenNanoSec := endTime.Sub(startTime)
	intervalSlot := IntervalSlot{TimePos: startTime, TimeLen: int64(timeLenNanoSec / time.Second)}
	return &intervalSlot, nil
}
