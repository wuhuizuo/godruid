package merge

import (
	"errors"
	"reflect"
)

const (
	// CompareErr not compariable
	CompareErr = int8(-2)
	// CompareLT left < right
	CompareLT = int8(-1)
	// CompareEQ left == right
	CompareEQ = int8(0)
	// CompareGT left > right
	CompareGT = int8(1)
)

// MapCompare compare two maps
func MapCompare(compareKeys []string, mapA, maptB map[string]interface{}) (int8, error) {
	var ret int8
	for _, k := range compareKeys {
		vA, okA := mapA[k]
		vB, okB := maptB[k]
		switch {
		case !okA && !okB: // A,B都无Key
			continue
		case !okA || vA == nil: // A 无Key, B有Key
			return CompareLT, nil
		case !okB || vB == nil: // A 有Key, B无Key
			return CompareGT, nil
		case vA == nil && vB == nil:
			continue
		default:
			cRet, cErr := interfaceCompare(vA, vB)
			if cErr != nil {
				return cRet, cErr
			}
			if cRet != CompareEQ {
				return cRet, cErr
			}
		}
	}
	return ret, nil
}

// interfaceCompare 目前只能用于简单类型的比较
func interfaceCompare(va, vb interface{}) (int8, error) {
	if reflect.TypeOf(va) == reflect.TypeOf(vb) {
		if reflect.DeepEqual(va, vb) {
			return CompareEQ, nil
		}

		ret := CompareLT
		switch va.(type) {
		case byte:
			if va.(byte) > vb.(byte) {
				ret = CompareGT
			}
		case int8:
			if va.(int8) > vb.(int8) {
				ret = CompareGT
			}
		case int:
			if va.(int) > vb.(int) {
				ret = CompareGT
			}
		case int32:
			if va.(int32) > vb.(int32) {
				ret = CompareGT
			}
		case int64:
			if va.(int64) > vb.(int64) {
				ret = CompareGT
			}
		case float32:
			if va.(float32) > vb.(float32) {
				ret = CompareGT
			}
		case float64:
			if va.(float64) > vb.(float64) {
				ret = CompareGT
			}
		case string:
			if va.(string) > vb.(string) {
				ret = CompareGT
			}
		default:
			return CompareErr, errors.New("no comparing support of the value type")
		}
		return ret, nil
	}
	return CompareErr, errors.New("not same types in interface{}")
}
