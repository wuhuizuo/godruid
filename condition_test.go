package godruid

import "testing"

func TestCondition_Match(t *testing.T) {
	tests := []struct {
		name string
		c    *Condition
		args interface{}
		want bool
	}{
		{"bool--eql--true--1", &Condition{Op: ConditionOpEql, Value: true}, true, true},
		{"bool--eql--true--2", &Condition{Op: ConditionOpEql, Value: true}, false, false},
		{"bool--eql--false--1", &Condition{Op: ConditionOpEql, Value: false}, false, true},
		{"bool--eql--false--2", &Condition{Op: ConditionOpEql, Value: false}, true, false},

		{"bool--notEql--true--1", &Condition{Op: ConditionOpNotEql, Value: true}, true, false},
		{"bool--notEql--true--2", &Condition{Op: ConditionOpNotEql, Value: true}, false, true},
		{"bool--notEql--false--1", &Condition{Op: ConditionOpNotEql, Value: false}, false, false},
		{"bool--notEql--false--2", &Condition{Op: ConditionOpNotEql, Value: false}, true, true},

		{"int--eql--1", &Condition{Op: ConditionOpEql, Value: int(1)}, int(1), true},
		{"int--eql--2", &Condition{Op: ConditionOpEql, Value: int(1)}, int(0), false},
		{"int--notEql--1", &Condition{Op: ConditionOpNotEql, Value: int(1)}, int(1), false},
		{"int--notEql--2", &Condition{Op: ConditionOpNotEql, Value: int(1)}, int(0), true},
		{"int-->--1", &Condition{Op: ConditionOpGT, Value: int(1)}, int(0), false},
		{"int-->--2", &Condition{Op: ConditionOpGT, Value: int(1)}, int(2), true},
		{"int-->=--1", &Condition{Op: ConditionOpGET, Value: int(1)}, int(0), false},
		{"int-->=--1", &Condition{Op: ConditionOpGET, Value: int(1)}, int(1), true},
		{"int-->=--2", &Condition{Op: ConditionOpGET, Value: int(1)}, int(2), true},
		{"int--<--1", &Condition{Op: ConditionOpLT, Value: int(1)}, int(0), true},
		{"int--<--2", &Condition{Op: ConditionOpLT, Value: int(1)}, int(2), false},
		{"int--<=--1", &Condition{Op: ConditionOpLET, Value: int(1)}, int(0), true},
		{"int--<=--1", &Condition{Op: ConditionOpLET, Value: int(1)}, int(1), true},
		{"int--<=--2", &Condition{Op: ConditionOpLET, Value: int(1)}, int(2), false},

		{"int8--eql--1", &Condition{Op: ConditionOpEql, Value: int8(1)}, int8(1), true},
		{"int8--eql--2", &Condition{Op: ConditionOpEql, Value: int8(1)}, int8(0), false},
		{"int8--notEql--1", &Condition{Op: ConditionOpNotEql, Value: int8(1)}, int8(1), false},
		{"int8--notEql--2", &Condition{Op: ConditionOpNotEql, Value: int8(1)}, int8(0), true},
		{"int8-->--1", &Condition{Op: ConditionOpGT, Value: int8(1)}, int8(0), false},
		{"int8-->--2", &Condition{Op: ConditionOpGT, Value: int8(1)}, int8(2), true},
		{"int8-->=--1", &Condition{Op: ConditionOpGET, Value: int8(1)}, int8(0), false},
		{"int8-->=--1", &Condition{Op: ConditionOpGET, Value: int8(1)}, int8(1), true},
		{"int8-->=--2", &Condition{Op: ConditionOpGET, Value: int8(1)}, int8(2), true},
		{"int8--<--1", &Condition{Op: ConditionOpLT, Value: int8(1)}, int8(0), true},
		{"int8--<--2", &Condition{Op: ConditionOpLT, Value: int8(1)}, int8(2), false},
		{"int8--<=--1", &Condition{Op: ConditionOpLET, Value: int8(1)}, int8(0), true},
		{"int8--<=--1", &Condition{Op: ConditionOpLET, Value: int8(1)}, int8(1), true},
		{"int8--<=--2", &Condition{Op: ConditionOpLET, Value: int8(1)}, int8(2), false},

		{"int16--eql--1", &Condition{Op: ConditionOpEql, Value: int16(1)}, int16(1), true},
		{"int16--eql--2", &Condition{Op: ConditionOpEql, Value: int16(1)}, int16(0), false},
		{"int16--notEql--1", &Condition{Op: ConditionOpNotEql, Value: int16(1)}, int16(1), false},
		{"int16--notEql--2", &Condition{Op: ConditionOpNotEql, Value: int16(1)}, int16(0), true},
		{"int16-->--1", &Condition{Op: ConditionOpGT, Value: int16(1)}, int16(0), false},
		{"int16-->--2", &Condition{Op: ConditionOpGT, Value: int16(1)}, int16(2), true},
		{"int16-->=--1", &Condition{Op: ConditionOpGET, Value: int16(1)}, int16(0), false},
		{"int16-->=--1", &Condition{Op: ConditionOpGET, Value: int16(1)}, int16(1), true},
		{"int16-->=--2", &Condition{Op: ConditionOpGET, Value: int16(1)}, int16(2), true},
		{"int16--<--1", &Condition{Op: ConditionOpLT, Value: int16(1)}, int16(0), true},
		{"int16--<--2", &Condition{Op: ConditionOpLT, Value: int16(1)}, int16(2), false},
		{"int16--<=--1", &Condition{Op: ConditionOpLET, Value: int16(1)}, int16(0), true},
		{"int16--<=--1", &Condition{Op: ConditionOpLET, Value: int16(1)}, int16(1), true},
		{"int16--<=--2", &Condition{Op: ConditionOpLET, Value: int16(1)}, int16(2), false},

		{"int32--eql--1", &Condition{Op: ConditionOpEql, Value: int32(1)}, int32(1), true},
		{"int32--eql--2", &Condition{Op: ConditionOpEql, Value: int32(1)}, int32(0), false},
		{"int32--notEql--1", &Condition{Op: ConditionOpNotEql, Value: int32(1)}, int32(1), false},
		{"int32--notEql--2", &Condition{Op: ConditionOpNotEql, Value: int32(1)}, int32(0), true},
		{"int32-->--1", &Condition{Op: ConditionOpGT, Value: int32(1)}, int32(0), false},
		{"int32-->--2", &Condition{Op: ConditionOpGT, Value: int32(1)}, int32(2), true},
		{"int32-->=--1", &Condition{Op: ConditionOpGET, Value: int32(1)}, int32(0), false},
		{"int32-->=--1", &Condition{Op: ConditionOpGET, Value: int32(1)}, int32(1), true},
		{"int32-->=--2", &Condition{Op: ConditionOpGET, Value: int32(1)}, int32(2), true},
		{"int32--<--1", &Condition{Op: ConditionOpLT, Value: int32(1)}, int32(0), true},
		{"int32--<--2", &Condition{Op: ConditionOpLT, Value: int32(1)}, int32(2), false},
		{"int32--<=--1", &Condition{Op: ConditionOpLET, Value: int32(1)}, int32(0), true},
		{"int32--<=--1", &Condition{Op: ConditionOpLET, Value: int32(1)}, int32(1), true},
		{"int32--<=--2", &Condition{Op: ConditionOpLET, Value: int32(1)}, int32(2), false},

		{"int64--eql--1", &Condition{Op: ConditionOpEql, Value: int64(1)}, int64(1), true},
		{"int64--eql--2", &Condition{Op: ConditionOpEql, Value: int64(1)}, int64(0), false},
		{"int64--notEql--1", &Condition{Op: ConditionOpNotEql, Value: int64(1)}, int64(1), false},
		{"int64--notEql--2", &Condition{Op: ConditionOpNotEql, Value: int64(1)}, int64(0), true},
		{"int64-->--1", &Condition{Op: ConditionOpGT, Value: int64(1)}, int64(0), false},
		{"int64-->--2", &Condition{Op: ConditionOpGT, Value: int64(1)}, int64(2), true},
		{"int64-->=--1", &Condition{Op: ConditionOpGET, Value: int64(1)}, int64(0), false},
		{"int64-->=--1", &Condition{Op: ConditionOpGET, Value: int64(1)}, int64(1), true},
		{"int64-->=--2", &Condition{Op: ConditionOpGET, Value: int64(1)}, int64(2), true},
		{"int64--<--1", &Condition{Op: ConditionOpLT, Value: int64(1)}, int64(0), true},
		{"int64--<--2", &Condition{Op: ConditionOpLT, Value: int64(1)}, int64(2), false},
		{"int64--<=--1", &Condition{Op: ConditionOpLET, Value: int64(1)}, int64(0), true},
		{"int64--<=--1", &Condition{Op: ConditionOpLET, Value: int64(1)}, int64(1), true},
		{"int64--<=--2", &Condition{Op: ConditionOpLET, Value: int64(1)}, int64(2), false},

		{"uint--eql--1", &Condition{Op: ConditionOpEql, Value: uint(1)}, uint(1), true},
		{"uint--eql--2", &Condition{Op: ConditionOpEql, Value: uint(1)}, uint(0), false},
		{"uint--notEql--1", &Condition{Op: ConditionOpNotEql, Value: uint(1)}, uint(1), false},
		{"uint--notEql--2", &Condition{Op: ConditionOpNotEql, Value: uint(1)}, uint(0), true},
		{"uint-->--1", &Condition{Op: ConditionOpGT, Value: uint(1)}, uint(0), false},
		{"uint-->--2", &Condition{Op: ConditionOpGT, Value: uint(1)}, uint(2), true},
		{"uint-->=--1", &Condition{Op: ConditionOpGET, Value: uint(1)}, uint(0), false},
		{"uint-->=--1", &Condition{Op: ConditionOpGET, Value: uint(1)}, uint(1), true},
		{"uint-->=--2", &Condition{Op: ConditionOpGET, Value: uint(1)}, uint(2), true},
		{"uint--<--1", &Condition{Op: ConditionOpLT, Value: uint(1)}, uint(0), true},
		{"uint--<--2", &Condition{Op: ConditionOpLT, Value: uint(1)}, uint(2), false},
		{"uint--<=--1", &Condition{Op: ConditionOpLET, Value: uint(1)}, uint(0), true},
		{"uint--<=--1", &Condition{Op: ConditionOpLET, Value: uint(1)}, uint(1), true},
		{"uint--<=--2", &Condition{Op: ConditionOpLET, Value: uint(1)}, uint(2), false},

		{"uint8--eql--1", &Condition{Op: ConditionOpEql, Value: uint8(1)}, uint8(1), true},
		{"uint8--eql--2", &Condition{Op: ConditionOpEql, Value: uint8(1)}, uint8(0), false},
		{"uint8--notEql--1", &Condition{Op: ConditionOpNotEql, Value: uint8(1)}, uint8(1), false},
		{"uint8--notEql--2", &Condition{Op: ConditionOpNotEql, Value: uint8(1)}, uint8(0), true},
		{"uint8-->--1", &Condition{Op: ConditionOpGT, Value: uint8(1)}, uint8(0), false},
		{"uint8-->--2", &Condition{Op: ConditionOpGT, Value: uint8(1)}, uint8(2), true},
		{"uint8-->=--1", &Condition{Op: ConditionOpGET, Value: uint8(1)}, uint8(0), false},
		{"uint8-->=--1", &Condition{Op: ConditionOpGET, Value: uint8(1)}, uint8(1), true},
		{"uint8-->=--2", &Condition{Op: ConditionOpGET, Value: uint8(1)}, uint8(2), true},
		{"uint8--<--1", &Condition{Op: ConditionOpLT, Value: uint8(1)}, uint8(0), true},
		{"uint8--<--2", &Condition{Op: ConditionOpLT, Value: uint8(1)}, uint8(2), false},
		{"uint8--<=--1", &Condition{Op: ConditionOpLET, Value: uint8(1)}, uint8(0), true},
		{"uint8--<=--1", &Condition{Op: ConditionOpLET, Value: uint8(1)}, uint8(1), true},
		{"uint8--<=--2", &Condition{Op: ConditionOpLET, Value: uint8(1)}, uint8(2), false},

		{"uint16--eql--1", &Condition{Op: ConditionOpEql, Value: uint16(1)}, uint16(1), true},
		{"uint16--eql--2", &Condition{Op: ConditionOpEql, Value: uint16(1)}, uint16(0), false},
		{"uint16--notEql--1", &Condition{Op: ConditionOpNotEql, Value: uint16(1)}, uint16(1), false},
		{"uint16--notEql--2", &Condition{Op: ConditionOpNotEql, Value: uint16(1)}, uint16(0), true},
		{"uint16-->--1", &Condition{Op: ConditionOpGT, Value: uint16(1)}, uint16(0), false},
		{"uint16-->--2", &Condition{Op: ConditionOpGT, Value: uint16(1)}, uint16(2), true},
		{"uint16-->=--1", &Condition{Op: ConditionOpGET, Value: uint16(1)}, uint16(0), false},
		{"uint16-->=--1", &Condition{Op: ConditionOpGET, Value: uint16(1)}, uint16(1), true},
		{"uint16-->=--2", &Condition{Op: ConditionOpGET, Value: uint16(1)}, uint16(2), true},
		{"uint16--<--1", &Condition{Op: ConditionOpLT, Value: uint16(1)}, uint16(0), true},
		{"uint16--<--2", &Condition{Op: ConditionOpLT, Value: uint16(1)}, uint16(2), false},
		{"uint16--<=--1", &Condition{Op: ConditionOpLET, Value: uint16(1)}, uint16(0), true},
		{"uint16--<=--1", &Condition{Op: ConditionOpLET, Value: uint16(1)}, uint16(1), true},
		{"uint16--<=--2", &Condition{Op: ConditionOpLET, Value: uint16(1)}, uint16(2), false},

		{"uint32--eql--1", &Condition{Op: ConditionOpEql, Value: uint32(1)}, uint32(1), true},
		{"uint32--eql--2", &Condition{Op: ConditionOpEql, Value: uint32(1)}, uint32(0), false},
		{"uint32--notEql--1", &Condition{Op: ConditionOpNotEql, Value: uint32(1)}, uint32(1), false},
		{"uint32--notEql--2", &Condition{Op: ConditionOpNotEql, Value: uint32(1)}, uint32(0), true},
		{"uint32-->--1", &Condition{Op: ConditionOpGT, Value: uint32(1)}, uint32(0), false},
		{"uint32-->--2", &Condition{Op: ConditionOpGT, Value: uint32(1)}, uint32(2), true},
		{"uint32-->=--1", &Condition{Op: ConditionOpGET, Value: uint32(1)}, uint32(0), false},
		{"uint32-->=--1", &Condition{Op: ConditionOpGET, Value: uint32(1)}, uint32(1), true},
		{"uint32-->=--2", &Condition{Op: ConditionOpGET, Value: uint32(1)}, uint32(2), true},
		{"uint32--<--1", &Condition{Op: ConditionOpLT, Value: uint32(1)}, uint32(0), true},
		{"uint32--<--2", &Condition{Op: ConditionOpLT, Value: uint32(1)}, uint32(2), false},
		{"uint32--<=--1", &Condition{Op: ConditionOpLET, Value: uint32(1)}, uint32(0), true},
		{"uint32--<=--1", &Condition{Op: ConditionOpLET, Value: uint32(1)}, uint32(1), true},
		{"uint32--<=--2", &Condition{Op: ConditionOpLET, Value: uint32(1)}, uint32(2), false},

		{"uint64--eql--1", &Condition{Op: ConditionOpEql, Value: uint64(1)}, uint64(1), true},
		{"uint64--eql--2", &Condition{Op: ConditionOpEql, Value: uint64(1)}, uint64(0), false},
		{"uint64--notEql--1", &Condition{Op: ConditionOpNotEql, Value: uint64(1)}, uint64(1), false},
		{"uint64--notEql--2", &Condition{Op: ConditionOpNotEql, Value: uint64(1)}, uint64(0), true},
		{"uint64-->--1", &Condition{Op: ConditionOpGT, Value: uint64(1)}, uint64(0), false},
		{"uint64-->--2", &Condition{Op: ConditionOpGT, Value: uint64(1)}, uint64(2), true},
		{"uint64-->=--1", &Condition{Op: ConditionOpGET, Value: uint64(1)}, uint64(0), false},
		{"uint64-->=--1", &Condition{Op: ConditionOpGET, Value: uint64(1)}, uint64(1), true},
		{"uint64-->=--2", &Condition{Op: ConditionOpGET, Value: uint64(1)}, uint64(2), true},
		{"uint64--<--1", &Condition{Op: ConditionOpLT, Value: uint64(1)}, uint64(0), true},
		{"uint64--<--2", &Condition{Op: ConditionOpLT, Value: uint64(1)}, uint64(2), false},
		{"uint64--<=--1", &Condition{Op: ConditionOpLET, Value: uint64(1)}, uint64(0), true},
		{"uint64--<=--1", &Condition{Op: ConditionOpLET, Value: uint64(1)}, uint64(1), true},
		{"uint64--<=--2", &Condition{Op: ConditionOpLET, Value: uint64(1)}, uint64(2), false},

		{"float32--eql--1", &Condition{Op: ConditionOpEql, Value: float32(1)}, float32(1), true},
		{"float32--eql--2", &Condition{Op: ConditionOpEql, Value: float32(1)}, float32(0), false},
		{"float32--notEql--1", &Condition{Op: ConditionOpNotEql, Value: float32(1)}, float32(1), false},
		{"float32--notEql--2", &Condition{Op: ConditionOpNotEql, Value: float32(1)}, float32(0), true},
		{"float32-->--1", &Condition{Op: ConditionOpGT, Value: float32(1)}, float32(0), false},
		{"float32-->--2", &Condition{Op: ConditionOpGT, Value: float32(1)}, float32(2), true},
		{"float32-->=--1", &Condition{Op: ConditionOpGET, Value: float32(1)}, float32(0), false},
		{"float32-->=--1", &Condition{Op: ConditionOpGET, Value: float32(1)}, float32(1), true},
		{"float32-->=--2", &Condition{Op: ConditionOpGET, Value: float32(1)}, float32(2), true},
		{"float32--<--1", &Condition{Op: ConditionOpLT, Value: float32(1)}, float32(0), true},
		{"float32--<--2", &Condition{Op: ConditionOpLT, Value: float32(1)}, float32(2), false},
		{"float32--<=--1", &Condition{Op: ConditionOpLET, Value: float32(1)}, float32(0), true},
		{"float32--<=--1", &Condition{Op: ConditionOpLET, Value: float32(1)}, float32(1), true},
		{"float32--<=--2", &Condition{Op: ConditionOpLET, Value: float32(1)}, float32(2), false},

		{"float64--eql--1", &Condition{Op: ConditionOpEql, Value: float64(1)}, float64(1), true},
		{"float64--eql--2", &Condition{Op: ConditionOpEql, Value: float64(1)}, float64(0), false},
		{"float64--notEql--1", &Condition{Op: ConditionOpNotEql, Value: float64(1)}, float64(1), false},
		{"float64--notEql--2", &Condition{Op: ConditionOpNotEql, Value: float64(1)}, float64(0), true},
		{"float64-->--1", &Condition{Op: ConditionOpGT, Value: float64(1)}, float64(0), false},
		{"float64-->--2", &Condition{Op: ConditionOpGT, Value: float64(1)}, float64(2), true},
		{"float64-->=--1", &Condition{Op: ConditionOpGET, Value: float64(1)}, float64(0), false},
		{"float64-->=--1", &Condition{Op: ConditionOpGET, Value: float64(1)}, float64(1), true},
		{"float64-->=--2", &Condition{Op: ConditionOpGET, Value: float64(1)}, float64(2), true},
		{"float64--<--1", &Condition{Op: ConditionOpLT, Value: float64(1)}, float64(0), true},
		{"float64--<--2", &Condition{Op: ConditionOpLT, Value: float64(1)}, float64(2), false},
		{"float64--<=--1", &Condition{Op: ConditionOpLET, Value: float64(1)}, float64(0), true},
		{"float64--<=--1", &Condition{Op: ConditionOpLET, Value: float64(1)}, float64(1), true},
		{"float64--<=--2", &Condition{Op: ConditionOpLET, Value: float64(1)}, float64(2), false},

		{"string--eql--1", &Condition{Op: ConditionOpEql, Value: "1"}, "1", true},
		{"string--eql--2", &Condition{Op: ConditionOpEql, Value: "1"}, "", false},
		{"string--notEql--1", &Condition{Op: ConditionOpNotEql, Value: "1"}, "1", false},
		{"string--notEql--2", &Condition{Op: ConditionOpNotEql, Value: "1"}, "", true},
		{"string-->--1", &Condition{Op: ConditionOpGT, Value: "1"}, "0", false},
		{"string-->--2", &Condition{Op: ConditionOpGT, Value: "1"}, "2", true},
		{"string-->=--1", &Condition{Op: ConditionOpGET, Value: "1"}, "0", false},
		{"string-->=--1", &Condition{Op: ConditionOpGET, Value: "1"}, "1", true},
		{"string-->=--2", &Condition{Op: ConditionOpGET, Value: "1"}, "2", true},
		{"string--<--1", &Condition{Op: ConditionOpLT, Value: "1"}, "0", true},
		{"string--<--2", &Condition{Op: ConditionOpLT, Value: "1"}, "2", false},
		{"string--<=--1", &Condition{Op: ConditionOpLET, Value: "1"}, "0", true},
		{"string--<=--1", &Condition{Op: ConditionOpLET, Value: "1"}, "1", true},
		{"string--<=--2", &Condition{Op: ConditionOpLET, Value: "1"}, "2", false},

		{"other--eql--1", &Condition{Op: ConditionOpEql, Value: "1"}, 1, false},
		{"other--eql--2", &Condition{Op: ConditionOpEql, Value: "1"}, nil, false},
		{"other--notEql--1", &Condition{Op: ConditionOpNotEql, Value: 1}, 1.0, false},
		{"other--notEql--2", &Condition{Op: ConditionOpNotEql, Value: 1}, nil, false},
		{"other-->--1", &Condition{Op: ConditionOpGT, Value: "1"}, 0, false},
		{"other-->--2", &Condition{Op: ConditionOpGT, Value: "1"}, 2, false},
		{"other-->=--1", &Condition{Op: ConditionOpGET, Value: 1}, "0", false},
		{"other-->=--1", &Condition{Op: ConditionOpGET, Value: 1}, "1", false},
		{"other-->=--2", &Condition{Op: ConditionOpGET, Value: 1}, "2", false},
		{"other--<--1", &Condition{Op: ConditionOpLT, Value: 1}, 0.0, true},
		{"other--<--2", &Condition{Op: ConditionOpLT, Value: 1}, 2.0, false},
		{"other--<=--1", &Condition{Op: ConditionOpLET, Value: 1.1}, "0", false},
		{"other--<=--1", &Condition{Op: ConditionOpLET, Value: 1.1}, "1", false},
		{"other--<=--2", &Condition{Op: ConditionOpLET, Value: 1.1}, "2", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.Match(tt.args); got != tt.want {
				t.Errorf("Condition.Match() = %v, want %v", got, tt.want)
			}
		})
	}
}
