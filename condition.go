package godruid

import (
	"reflect"
)

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
	// ConditionOpMapInclude map part include juge: for map
	ConditionOpMapInclude = "⊇"
	// ConditionOpSetInclude set part include juge: for array, set
	ConditionOpSetInclude = ConditionOpMapInclude
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
		return c.MatchNumber(data)
	case string:
		return c.MatchString(data)
	case map[string]interface{}:
		return c.MatchMap(data)
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

// MatchNumber for number dest value
func (c *Condition) MatchNumber(data interface{}) bool {
	switch data.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return c.matchFloat64(numberFloat64(data), numberFloat64(c.Value))
	default:
		return false
	}
}

// MatchString for string dest value
func (c *Condition) MatchString(data interface{}) bool {
	switch data.(type) {
	case string:
		return c.matchString(data.(string), c.Value.(string))
	default:
		return false
	}
}

// MatchMap for map dest value
func (c *Condition) MatchMap(data interface{}) bool {
	if data == nil {
		return false
	}
	m, mok := c.Value.(map[string]interface{})
	if !mok {
		return false
	}
	d, dok := data.(map[string]interface{})

	return dok && c.matchMap(d, m)
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

func (c Condition) matchMap(data, pattern map[string]interface{}) bool {
	switch c.Op {
	case ConditionOpMapInclude:
		ret := true
		for k, pv := range pattern {
			v, ok := data[k]
			if !ok || !reflect.DeepEqual(pv, v) {
				ret = false
				break
			}
		}
		return ret
	default:
		return false
	}
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
