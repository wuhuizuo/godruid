package godruid

type Granlarity interface{}

type SimpleGran string

const (
	GranAll        SimpleGran = "all"
	GranNone       SimpleGran = "none"
	GranMinute     SimpleGran = "minute"
	GranFifteenMin SimpleGran = "fifteen_minute"
	GranThirtyMin  SimpleGran = "thirty_minute"
	GranHour       SimpleGran = "hour"
	GranDay        SimpleGran = "day"
)

type granDuration struct {
	Type     string `json:"type"`
	Duration string `json:"duration"`
	Origin   string `json:"origin,omitempty"`
}

type granPeriod struct {
	Type     string `json:"type"`
	Period   string `json:"period"`
	TimeZone string `json:"timeZone,omitempty"`
	Origin   string `json:"origin,omitempty"`
}

// GranPeriod new Period Granularity
func GranPeriod(period string, timeZone string, origin string) granPeriod {
	return granPeriod{
		Type:     "period",
		Period:   period,
		TimeZone: timeZone,
		Origin:   origin,
	}
}

// GranDuration new Duration Granularity
func GranDuration(duration string, origin string) granDuration {
	return granDuration{
		Type:     "duration",
		Duration: duration,
		Origin:   origin,
	}
}
