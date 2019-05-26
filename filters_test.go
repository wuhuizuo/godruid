package godruid

import (
	"reflect"
	"testing"
)

func TestFilter_ToConditions(t *testing.T) {
	tests := []struct {
		name    string
		f       Filter
		want    []Condition
		wantErr bool
	}{
		{"selector-number-1", *FilterSelector("abc", 123), []Condition{Condition{FieldName: "abc", Op: "==", Value: 123}}, false},
		{"selector-string-1", *FilterSelector("abc", "abc"), []Condition{Condition{FieldName: "abc", Op: "==", Value: "abc"}}, false},
		{"bound-lower-number-1", *FilterLowerBound("abc", "number", "123", false), []Condition{Condition{FieldName: "abc", Op: ">=", Value: "123"}}, false},
		{"bound-lower-number-2", *FilterLowerBound("abc", "number", "123", true), []Condition{Condition{FieldName: "abc", Op: ">", Value: "123"}}, false},
		{"bound-upper-number-1", *FilterUpperBound("abc", "number", "123", false), []Condition{Condition{FieldName: "abc", Op: "<=", Value: "123"}}, false},
		{"bound-upper-number-2", *FilterUpperBound("abc", "number", "123", true), []Condition{Condition{FieldName: "abc", Op: "<", Value: "123"}}, false},
		{
			"bound-lower-upper-1",
			*FilterLowerUpperBound("abc", "number", "123", false, "456", false),
			[]Condition{
				Condition{FieldName: "abc", Op: ">=", Value: "123"},
				Condition{FieldName: "abc", Op: "<=", Value: "456"},
			},
			false,
		},
		{
			"and-1",
			*FilterAnd(FilterLowerUpperBound("abc", "number", "123", false, "456", false), FilterSelector("def", 123)),
			[]Condition{
				Condition{FieldName: "abc", Op: ">=", Value: "123"},
				Condition{FieldName: "abc", Op: "<=", Value: "456"},
				Condition{FieldName: "def", Op: "==", Value: 123},
			},
			false,
		},
		{
			"or",
			*FilterOr(FilterSelector("abc", "123"), FilterSelector("def", "123")),
			nil,
			true,
		},
		{
			"not",
			*FilterNot(FilterLowerUpperBound("abc", "number", "123", false, "456", false)),
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.f.ToConditions()
			if (err != nil) != tt.wantErr {
				t.Errorf("Filter.ToConditions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Filter.ToConditions() = %v, want %v", got, tt.want)
			}
		})
	}
}