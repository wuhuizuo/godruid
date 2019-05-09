package merge

import (
	"reflect"
	"testing"
)

func TestSum(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want interface{}
	}{
		{"int8", []interface{}{int8(1), int8(2), int8(3)}, int8(6)},
		{"int16", []interface{}{int16(1), int16(2), int16(3)}, int16(6)},
		{"int32", []interface{}{int32(1), int32(2), int32(3)}, int32(6)},
		{"int", []interface{}{int(1), int(2), int(3)}, int(6)},
		{"int64", []interface{}{int64(1), int64(2), int64(3)}, int64(6)},
		{"float32", []interface{}{float32(1), float32(2), float32(3)}, float32(6)},
		{"float64", []interface{}{float64(1), float64(2), float64(3)}, float64(6)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Sum(tt.vals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want interface{}
	}{
		{"int8", []interface{}{int8(1), int8(2), int8(3)}, int8(1)},
		{"int16", []interface{}{int16(1), int16(2), int16(3)}, int16(1)},
		{"int32", []interface{}{int32(1), int32(2), int32(3)}, int32(1)},
		{"int", []interface{}{int(1), int(2), int(3)}, int(1)},
		{"int64", []interface{}{int64(1), int64(2), int64(3)}, int64(1)},
		{"float32", []interface{}{float32(1), float32(2), float32(3)}, float32(1)},
		{"float64", []interface{}{float64(1), float64(2), float64(3)}, float64(1)},
		{"string", []interface{}{"a", "A", "c"}, "A"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Min(tt.vals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want interface{}
	}{
		{"int8", []interface{}{int8(1), int8(3), int8(2)}, int8(3)},
		{"int16", []interface{}{int16(1), int16(3), int16(2)}, int16(3)},
		{"int32", []interface{}{int32(1), int32(2), int32(3)}, int32(3)},
		{"int", []interface{}{int(1), int(2), int(3)}, int(3)},
		{"int64", []interface{}{int64(1), int64(2), int64(3)}, int64(3)},
		{"float32", []interface{}{float32(1), float32(2), float32(3)}, float32(3)},
		{"float64", []interface{}{float64(1), float64(2), float64(3)}, float64(3)},
		{"string", []interface{}{"a", "A", "c"}, "c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Max(tt.vals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSub(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want interface{}
	}{
		{"int8", []interface{}{int8(1), int8(2), int8(3)}, int8(-4)},
		{"int16", []interface{}{int16(1), int16(2), int16(3)}, int16(-4)},
		{"int32", []interface{}{int32(1), int32(2), int32(3)}, int32(-4)},
		{"int", []interface{}{int(1), int(2), int(3)}, int(-4)},
		{"int64", []interface{}{int64(1), int64(2), int64(3)}, int64(-4)},
		{"float32", []interface{}{float32(1), float32(2), float32(3)}, float32(-4)},
		{"float64", []interface{}{float64(1), float64(2), float64(3)}, float64(-4)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Sub(tt.vals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Sub() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiply(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want interface{}
	}{
		{"int8", []interface{}{int8(10), int8(2), int8(3)}, int64(60)},
		{"int16", []interface{}{int16(10), int16(2), int16(3)}, int64(60)},
		{"int32", []interface{}{int32(10), int32(2), int32(3)}, int64(60)},
		{"int", []interface{}{int(10), int(2), int(3)}, int64(60)},
		{"int64", []interface{}{int64(10), int64(2), int64(3)}, int64(60)},
		{"float32", []interface{}{float32(10), float32(2), float32(3)}, float64(60)},
		{"float64", []interface{}{float64(10), float64(2), float64(3)}, float64(60)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Multiply(tt.vals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Multiply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDivide(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want interface{}
	}{
		{"int8", []interface{}{int8(10), int8(2), int8(2)}, float32(2.5)},
		{"int16", []interface{}{int16(10), int16(2), int16(2)}, float32(2.5)},
		{"int32", []interface{}{int32(10), int32(2), int32(2)}, float32(2.5)},
		{"int", []interface{}{int(10), int(2), int(2)}, float64(2.5)},
		{"int64", []interface{}{int64(10), int64(2), int64(2)}, float64(2.5)},
		{"float32", []interface{}{float32(10), float32(2), float32(2)}, float32(2.5)},
		{"float64", []interface{}{float64(10), float64(2), float64(2)}, float64(2.5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Divide(tt.vals...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Divide() = %v(%T), want %v(%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestPostAggComputeArithmetic(t *testing.T) {
	type args struct {
		data          map[string]interface{}
		arithmeticExp string
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{"+_int8", args{map[string]interface{}{"a": int8(1), "b": int8(2)}, "a + b"}, float64(3)},
		{"-_int8", args{map[string]interface{}{"a": int8(1), "b": int8(2)}, "a - b"}, float64(-1)},
		{"*_int8", args{map[string]interface{}{"a": int8(1), "b": int8(2)}, "a * b"}, float64(2)},
		{"/_int8", args{map[string]interface{}{"a": int8(1), "b": int8(2)}, "a / b"}, float64(0.5)},

		{"+_int16", args{map[string]interface{}{"a": int16(1), "b": int16(2)}, "a + b"}, float64(3)},
		{"-_int16", args{map[string]interface{}{"a": int16(1), "b": int16(2)}, "a - b"}, float64(-1)},
		{"*_int16", args{map[string]interface{}{"a": int16(1), "b": int16(2)}, "a * b"}, float64(2)},
		{"/_int16", args{map[string]interface{}{"a": int16(1), "b": int16(2)}, "a / b"}, float64(0.5)},

		{"+_int", args{map[string]interface{}{"a": 1, "b": 2}, "a + b"}, float64(3)},
		{"-_int", args{map[string]interface{}{"a": 1, "b": 2}, "a - b"}, float64(-1)},
		{"*_int", args{map[string]interface{}{"a": 1, "b": 2}, "a * b"}, float64(2)},
		{"/_int", args{map[string]interface{}{"a": 1, "b": 2}, "a / b"}, float64(0.5)},

		{"+_int32", args{map[string]interface{}{"a": int32(1), "b": int32(2)}, "a + b"}, float64(3)},
		{"-_int32", args{map[string]interface{}{"a": int32(1), "b": int32(2)}, "a - b"}, float64(-1)},
		{"*_int32", args{map[string]interface{}{"a": int32(1), "b": int32(2)}, "a * b"}, float64(2)},
		{"/_int32", args{map[string]interface{}{"a": int32(1), "b": int32(2)}, "a / b"}, float64(0.5)},

		{"+_int64", args{map[string]interface{}{"a": int64(1), "b": int64(2)}, "a + b"}, float64(3)},
		{"-_int64", args{map[string]interface{}{"a": int64(1), "b": int64(2)}, "a - b"}, float64(-1)},
		{"*_int64", args{map[string]interface{}{"a": int64(1), "b": int64(2)}, "a * b"}, float64(2)},
		{"/_int64", args{map[string]interface{}{"a": int64(1), "b": int64(2)}, "a / b"}, float64(0.5)},

		{"+_float64", args{map[string]interface{}{"a": float64(1), "b": float64(2)}, "a + b"}, float64(3)},
		{"-_float64", args{map[string]interface{}{"a": float64(1), "b": float64(2)}, "a - b"}, float64(-1)},
		{"*_float64", args{map[string]interface{}{"a": float64(1), "b": float64(2)}, "a * b"}, float64(2)},
		{"/_float64", args{map[string]interface{}{"a": float64(1), "b": float64(2)}, "a / b"}, float64(0.5)},

		{"-_float64-2", args{map[string]interface{}{"a": float64(1), "b": float64(0.2)}, "1 - b"}, float64(0.8)},
		{"-/_float64-2", args{map[string]interface{}{"a": float64(1), "b": float64(2)}, "1 - (a / b)"}, float64(0.5)},
		// 1 - a / b
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PostAggComputeArithmetic(tt.args.data, tt.args.arithmeticExp)
			if err != nil {
				t.Errorf("PostAggComputeArithmeticx() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PostAggComputeArithmetic() = %v(%T), want %v(%T)", got, got, tt.want, tt.want)
			}
		})
	}
}
