package merge

import (
	"fmt"
	"reflect"
	govaluate "github.com/wuhuizuo/govaluate"
)

// Sum get sum from values
func Sum(vals ...interface{}) interface{} {
	if len(vals) == 1 {
		return vals[0]
	}
	if len(vals) > 1 {
		t := reflect.TypeOf(vals[0])

		for _, v := range vals[1:] {
			if t != reflect.TypeOf(v) {
				panic("type are not same")
			}
		}

		switch vals[0].(type) {
		case int8:
			return sumInt8(vals...)
		case int16:
			return sumInt16(vals...)
		case int32:
			return sumInt32(vals...)
		case int:
			return sumInt(vals...)
		case int64:
			return sumInt64(vals...)
		case float32:
			return sumFloat32(vals...)
		case float64:
			return sumFloat64(vals...)
		default:
			panic(fmt.Sprintf("not support type:%T", vals[0]))
		}

	}
	return nil
}

// Add for + Arithmetic
func Add(vals ...interface{}) interface{} {
	return Sum(vals...)
}

// Sub for - Arithmetic
func Sub(vals ...interface{}) interface{} {
	if len(vals) == 1 {
		return vals[0]
	}
	if len(vals) > 1 {
		t := reflect.TypeOf(vals[0])

		for _, v := range vals[1:] {
			if t != reflect.TypeOf(v) {
				panic("type are not same")
			}
		}

		switch vals[0].(type) {
		case int8:
			return subInt8(vals...)
		case int16:
			return subInt16(vals...)
		case int32:
			return subInt32(vals...)
		case int:
			return subInt(vals...)
		case int64:
			return subInt64(vals...)
		case float32:
			return subFloat32(vals...)
		case float64:
			return subFloat64(vals...)
		default:
			panic(fmt.Sprintf("not support type:%T", vals[0]))
		}

	}

	return nil
}

// Multiply for * Arithmetic
func Multiply(vals ...interface{}) interface{} {
	if len(vals) == 1 {
		return vals[0]
	}
	if len(vals) > 1 {
		t := reflect.TypeOf(vals[0])

		for _, v := range vals[1:] {
			if t != reflect.TypeOf(v) {
				panic("type are not same")
			}
		}

		switch vals[0].(type) {
		case int8:
			return multiplyInt8(vals...)
		case int16:
			return multiplyInt16(vals...)
		case int32:
			return multiplyInt32(vals...)
		case int:
			return multiplyInt(vals...)
		case int64:
			return multiplyInt64(vals...)
		case float32:
			return multiplyFloat32(vals...)
		case float64:
			return multiplyFloat64(vals...)
		default:
			panic(fmt.Sprintf("not support type:%T", vals[0]))
		}
	}
	return nil
}

// Divide for ➗ Arithmetic
func Divide(vals ...interface{}) interface{} {
	if len(vals) == 1 {
		return vals[0]
	}
	if len(vals) > 1 {
		t := reflect.TypeOf(vals[0])

		for _, v := range vals[1:] {
			if t != reflect.TypeOf(v) {
				panic("type are not same")
			}
		}

		switch vals[0].(type) {
		case int8:
			return divideInt8(vals...)
		case int16:
			return divideInt16(vals...)
		case int32:
			return divideInt32(vals...)
		case int:
			return divideInt(vals...)
		case int64:
			return divideInt64(vals...)
		case float32:
			return divideFloat32(vals...)
		case float64:
			return divideFloat64(vals...)
		default:
			panic(fmt.Sprintf("not support type:%T", vals[0]))
		}
	}
	return nil
}

// Min get min from values
func Min(vals ...interface{}) interface{} {
	if len(vals) == 1 {
		return vals[0]
	}
	if len(vals) > 1 {
		t := reflect.TypeOf(vals[0])

		for _, v := range vals[1:] {
			if t != reflect.TypeOf(v) {
				panic("type are not same")
			}
		}

		switch vals[0].(type) {
		case int8:
			return minInt8(vals...)
		case int16:
			return minInt16(vals...)
		case int32:
			return minInt32(vals...)
		case int:
			return minInt(vals...)
		case int64:
			return minInt64(vals...)
		case float32:
			return minFloat32(vals...)
		case float64:
			return minFloat64(vals...)
		case string:
			return minString(vals...)
		default:
			panic(fmt.Sprintf("not support type:%T", vals[0]))
		}

	}
	return nil
}

// Max get max from values
func Max(vals ...interface{}) interface{} {
	if len(vals) == 1 {
		return vals[0]
	}
	if len(vals) > 1 {
		t := reflect.TypeOf(vals[0])

		for _, v := range vals[1:] {
			if t != reflect.TypeOf(v) {
				panic("type are not same")
			}
		}

		switch vals[0].(type) {
		case int8:
			return maxInt8(vals...)
		case int16:
			return maxInt16(vals...)
		case int32:
			return maxInt32(vals...)
		case int:
			return maxInt(vals...)
		case int64:
			return maxInt64(vals...)
		case float32:
			return maxFloat32(vals...)
		case float64:
			return maxFloat64(vals...)
		case string:
			return maxString(vals...)
		default:
			panic(fmt.Sprintf("not support type:%T", vals[0]))
		}

	}
	return nil
}

// PostAggComputeArithmetic post agg 算法
func PostAggComputeArithmetic(data map[string]interface{}, arithmeticExp string) (interface{}, error) {
	fmt.Println(arithmeticExp)
	exp, err := govaluate.NewEvaluableExpression(arithmeticExp)
	if err != nil {
		return nil , err
	}
	return exp.Evaluate(data)
}
