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

func TestQueryGroupBy_DimNames(t *testing.T) {
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
			if got := tt.q.DimNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.DimNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_AggNames(t *testing.T) {
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
			if got := tt.q.AggNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.AggNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_PostAggNames(t *testing.T) {
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
			if got := tt.q.PostAggNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.PostAggNames() = %v, want %v", got, tt.want)
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

func Test_postAggExp(t *testing.T) {
	pa := PostAggArithmetic("avg_xxx", "/", []PostAggregation{
		PostAggFieldAccessor("aggLongSum"),
		PostAggFieldAccessor("aggCount"),
	})
	tests := []struct {
		name string
		pg   PostAggregation
		want []string
	}{
		{"default", pa, []string{"/", "aggLongSum", "aggCount"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := postAggExp(tt.pg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postAggExp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_mergeQueryResult(t *testing.T) {
	type args struct {
		oResult []GroupbyItem
	}
	tests := []struct {
		name string
		q    *QueryGroupBy
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.q.mergeQueryResult(tt.args.oResult)
		})
	}
}

func TestQueryGroupBy_canMerge(t *testing.T) {
	tests := []struct {
		name    string
		q       *QueryGroupBy
		oq      *QueryGroupBy
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.q.canMerge(tt.oq); (err != nil) != tt.wantErr {
				t.Errorf("QueryGroupBy.canMerge() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestQueryGroupBy_aggTypes(t *testing.T) {
	tests := []struct {
		name string
		q    *QueryGroupBy
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.aggTypes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.aggTypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_postAggExps(t *testing.T) {
	tests := []struct {
		name string
		q    *QueryGroupBy
		want [][]string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.postAggExps(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.postAggExps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryGroupBy_postAggExpStrings(t *testing.T) {
	tests := []struct {
		name string
		q    *QueryGroupBy
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.postAggExpStrings(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryGroupBy.postAggExpStrings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mergeAgg(t *testing.T) {
	type args struct {
		aggType string
		aggVals []interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeAgg(tt.args.aggType, tt.args.aggVals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeAgg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_reComputePostAggs(t *testing.T) {
	type args struct {
		event        map[string]interface{}
		postAggNames []string
		postAggExps  [][]string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reComputePostAggs(tt.args.event, tt.args.postAggNames, tt.args.postAggExps)
		})
	}
}

func Test_reComputePostAgg(t *testing.T) {
	type args struct {
		event       map[string]interface{}
		postAggName string
		postAggExp  []string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reComputePostAgg(tt.args.event, tt.args.postAggName, tt.args.postAggExp)
		})
	}
}

func Test_eventCanMerge(t *testing.T) {
	type args struct {
		dims   []string
		event1 map[string]interface{}
		event2 map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eventCanMerge(tt.args.dims, tt.args.event1, tt.args.event2); got != tt.want {
				t.Errorf("eventCanMerge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mergeEvent(t *testing.T) {
	type args struct {
		eventA   map[string]interface{}
		eventB   map[string]interface{}
		aggNames []string
		aggTypes []string
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeEvent(tt.args.eventA, tt.args.eventB, tt.args.aggNames, tt.args.aggTypes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeEvent() = %v, want %v", got, tt.want)
			}
		})
	}
}
