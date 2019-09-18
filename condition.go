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
	switch c.Value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		switch data.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			return c.matchFloat64(numberFloat64(data), numberFloat64(c.Value))
		default:
			return false
		}
	case string:
		switch data.(type) {
		case string:
			return c.matchString(data.(string), c.Value.(string))
		default:
			return false
		}
	default:
		switch c.Op {
		case ConditionOpEql, ConditionOpEql2:
			return reflect.DeepEqual(c.Value, data)
		case ConditionOpNotEql:
			return !reflect.DeepEqual(c.Value, data)
		default:
			return false
		}
	}
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

func numberFloat64(x interface{}) float64 {
	v := reflect.ValueOf(x)
	switch x.(type) {
	case int, int8, int16, int32, int64:
		return float64(v.Int())
	case uint, uint8, uint16, uint32, uint64:
		return float64(v.Uint())
	case float32, float64:
		return v.Float()
	default:
		panic("not number type")
	}
}
