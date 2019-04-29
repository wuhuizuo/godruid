package godruid

import (
	"reflect"
	"testing"
	"time"
)

func Test_parseInterval(t *testing.T) {
	cstTimeZone, _ := time.LoadLocation("Local")
	tests := []struct {
		name     string
		interval string
		want     *IntervalSlot
		wantErr  bool
	}{
		{
			"min",
			"2019-04-29T00:00:00.000+08:00/2019-04-29T00:01:00.000+08:00",
			&IntervalSlot{TimePos: time.Date(2019, 4, 29, 0, 0, 0, 0, cstTimeZone), TimeLen: 60},
			false,
		},
		{
			"hour",
			"2019-04-29T00:00:00.000+08:00/2019-04-29T01:00:00.000+08:00",
			&IntervalSlot{TimePos: time.Date(2019, 4, 29, 0, 0, 0, 0, cstTimeZone), TimeLen: 3600},
			false,
		},
		{
			"day",
			"2019-04-29T00:00:00.000+08:00/2019-04-30T00:00:00.000+08:00",
			&IntervalSlot{TimePos: time.Date(2019, 4, 29, 0, 0, 0, 0, cstTimeZone), TimeLen: 3600 * 24},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInterval(tt.interval)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInterval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}
