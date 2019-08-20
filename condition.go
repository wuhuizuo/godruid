package godruid

import "reflect"

const (
	// ConditionOpEql equal juge
	ConditionOpEql = "="
	// ConditionOpEql2 equal juge, another format
	ConditionOpEql2 = "=="
	// ConditionOpNotEql not equal juge
	ConditionOpNotEql = "!="
	// ConditionOpGT > juge
	ConditionOpGT = ">"
	// ConditionOpGET >= juge
	ConditionOpGET = ">="
	// ConditionOpLT < juge
	ConditionOpLT = "<"
	// ConditionOpLET <= juge
	ConditionOpLET = "<="
)

// Condition cache query condition
// * copy from https://git.code.oa.com/flarezuo/miglib/blob/master/dcache/jce/DCache/Condition.go
type Condition struct {
	FieldName string      `json:"fieldName"`
	Op        string      `json:"op"`
	Value     interface{} `json:"value"`
}

// Match data?
func (c *Condition) Match(data interface{}) bool {
	if reflect.TypeOf(c.Value) != reflect.TypeOf(data) {
		return false
	}
	switch c.Op {
	case ConditionOpEql, ConditionOpEql2:
		return reflect.DeepEqual(c.Value, data)
	case ConditionOpNotEql:
		return !reflect.DeepEqual(c.Value, data)
	default:
		v := reflect.ValueOf(c.Value)
		d := reflect.ValueOf(data)
		switch c.Value.(type) {
		case int, int8, int16, int32, int64:
			return c.matchInt64(v.Int(), d.Int())
		case uint, uint8, uint16, uint32, uint64:
			return c.matchUint64(v.Uint(), d.Uint())
		case float32, float64:
			return c.matchFloat64(v.Float(), d.Float())
		case string:
			return c.matchString(v.String(), d.String())
		}
	}

	return false
}

func (c *Condition) matchInt64(x, y int64) bool {
	switch c.Op {
	case ConditionOpLT:
		return x < y
	case ConditionOpLET:
		return x <= y
	case ConditionOpGT:
		return x > y
	case ConditionOpGET:
		return x >= y
	case ConditionOpEql, ConditionOpEql2:
		return x == y
	case ConditionOpNotEql:
		return x != y
	}
	return false
}

func (c *Condition) matchUint64(x, y uint64) bool {
	switch c.Op {
	case ConditionOpLT:
		return x < y
	case ConditionOpLET:
		return x <= y
	case ConditionOpGT:
		return x > y
	case ConditionOpGET:
		return x >= y
	case ConditionOpEql, ConditionOpEql2:
		return x == y
	case ConditionOpNotEql:
		return x != y
	}
	return false
}

func (c *Condition) matchFloat64(x, y float64) bool {
	switch c.Op {
	case ConditionOpLT:
		return x < y
	case ConditionOpLET:
		return x <= y
	case ConditionOpGT:
		return x > y
	case ConditionOpGET:
		return x >= y
	case ConditionOpEql, ConditionOpEql2:
		return x == y
	case ConditionOpNotEql:
		return x != y
	}
	return false
}

func (c *Condition) matchString(x, y string) bool {
	switch c.Op {
	case ConditionOpLT:
		return x < y
	case ConditionOpLET:
		return x <= y
	case ConditionOpGT:
		return x > y
	case ConditionOpGET:
		return x >= y
	case ConditionOpEql, ConditionOpEql2:
		return x == y
	case ConditionOpNotEql:
		return x != y
	}
	return false
}
