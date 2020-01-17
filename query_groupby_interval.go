package godruid

import (
	"fmt"
	"strings"
	"time"
)

// IntervalSlot query interval slot item
type IntervalSlot struct {
	TimePos time.Time `json:"timePos"`
	TimeLen int64     `json:"timeLen"`
}

// ToInterval to interval string
func (i *IntervalSlot) ToInterval() string {
	startTimeStr := i.TimePos.Format(time.RFC3339)
	endTime := i.TimePos.Add(time.Duration(i.TimeLen) * time.Second)
	endTimeStr := endTime.Format(time.RFC3339)
	return fmt.Sprintf("%s/%s", startTimeStr, endTimeStr)
}

// DistributeIntervalSlots split intervals to whole days and hours
func (q *QueryGroupBy) DistributeIntervalSlots() ([]IntervalSlot, error) {
	ret := []IntervalSlot{}
	for _, i := range q.Intervals {
		intervalSlots, err := DistributeIntervals(i)
		if err != nil {
			return ret, err
		}
		for _, intervalSlot := range intervalSlots {
			ret = append(ret, intervalSlot)
		}
	}
	return ret, nil
}

// DistributeIntervals distribute interval to serval interval slots, minium to hour
func DistributeIntervals(interval string) ([]IntervalSlot, error) {
	ret := []IntervalSlot{}
	intervalSlot, err := ParseInterval(interval)
	if err == nil {
		// TODO: 按年，月，周
		dSi, dIs, dEi := DistributeDays(*intervalSlot)
		if dSi != nil {
			hSi, hIs, hEi := DistributeHours(*dSi)

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
			hSi, hIs, hEi := DistributeHours(*dEi)

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
	return ret, err
}

// DistributeDays distribute interval to serval day interval slots
func DistributeDays(intervalSlot IntervalSlot) (*IntervalSlot, []IntervalSlot, *IntervalSlot) {
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

// DistributeHours distribute interval to serval hour interval slots
func DistributeHours(intervalSlot IntervalSlot) (*IntervalSlot, []IntervalSlot, *IntervalSlot) {
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

func ParseInterval(interval string) (*IntervalSlot, error) {
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
