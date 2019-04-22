package godruid

import (
	"reflect"
	"testing"
)

func TestQueryGroupBy_Merge(t *testing.T) {
	tests := []struct {
		name    string
		q       *QueryGroupBy
		oq      *QueryGroupBy
		wantErr bool
	}{
		{
			"simple",
			&QueryGroupBy{Granularity: SimpleGran("all"), Intervals: []string{"2019-01-01T00:00:00/2019-01-02T00:00:00:00"}},
			&QueryGroupBy{Granularity: SimpleGran("all"), Intervals: []string{"2019-01-01T00:00:00/2019-01-03T00:00:00:00"}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.q.Merge(tt.oq); (err != nil) != tt.wantErr {
				t.Errorf("QueryGroupBy.Merge() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestQueryGroupBy_dimNames(t *testing.T) {
	tests := []struct {
		name string
		q    *QueryGroupBy
		want []string
	}{
		{
			"default",
			&QueryGroupBy{
				Dimensions: []DimSpec{"d1", Dimension{OutputName: "d2"}},
			},
			[]string{"d1", "d2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.dimNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.dimNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_aggNames(t *testing.T) {
	query := QueryGroupBy{Aggregations: []Aggregation{
		*AggCount("aggCount"),
		*AggLongSum("aggLongSum", "dimA"),
		*AggMin("aggMin", "dimB"),
		*AggDoubleMin("aggDoubleMin", "dimC"),
		*AggLongMin("aggLongMin", "dimD"),
		*AggMax("aggMax", "dimE"),
		*AggDoubleMax("aggDoubleMax", "dimF"),
		*AggLongMax("aggLongMax", "dimG"),
		AggFiltered(FilterSelector("dimH", "123"), AggCount("aggFiltered")),
	}}
	tests := []struct {
		name string
		q    *QueryGroupBy
		want []string
	}{
		{"default", &query, []string{"aggCount", "aggLongSum", "aggMin", "aggDoubleMin", "aggLongMin", "aggMax", "aggDoubleMax", "aggLongMax", "aggFiltered"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.aggNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.aggNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_aggType(t *testing.T) {
	countAgg := AggCount("AggCount")
	filter := FilterSelector("abc", "123")
	tests := []struct {
		name string
		agg  Aggregation
		want string
	}{
		{"AggCount", *countAgg, "count"},
		{"AggLongSum", *AggLongSum("sumA", "dimA"), "longSum"},
		{"AggDoubleSum", *AggDoubleSum("666A", "dimA"), "doubleSum"},
		{"AggMin", *AggMin("666A", "dimA"), "min"},
		{"AggDoubleMin", *AggDoubleMin("666A", "dimA"), "doubleMin"},
		{"AggLongMin", *AggLongMin("666A", "dimA"), "longMin"},
		{"AggMax", *AggMax("666A", "dimA"), "max"},
		{"AggDoubleMax", *AggDoubleMax("666A", "dimA"), "doubleMax"},
		{"AggLongMax", *AggLongMax("666A", "dimA"), "longMax"},
		{"AggFiltered", AggFiltered(filter, countAgg), "count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggType(tt.agg); got != tt.want {
				t.Errorf("aggType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_postAggNames(t *testing.T) {
	query := QueryGroupBy{
		Aggregations: []Aggregation{*AggCount("aggCount"), *AggLongSum("aggLongSum", "dimA")},
		PostAggregations: []PostAggregation{PostAggArithmetic("avg_xxx", "/", []PostAggregation{
			PostAggFieldAccessor("aggLongSum"),
			PostAggFieldAccessor("aggCount"),
		})},
	}

	tests := []struct {
		name string
		q    *QueryGroupBy
		want []string
	}{
		{"default", &query, []string{"avg_xxx"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.postAggNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.postAggNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_postAggExp(t *testing.T) {
	pa := PostAggArithmetic("avg_xxx", "/", []PostAggregation{
		PostAggFieldAccessor("aggLongSum"),
		PostAggFieldAccessor("aggCount"),
	})
	tests := []struct {
		name string
		pg   PostAggregation
		want string
	}{
		{"default", pa, "/:aggLongSum,aggCount"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := postAggExp(tt.pg); got != tt.want {
				t.Errorf("postAggExp() = %v, want %v", got, tt.want)
			}
		})
	}
}


