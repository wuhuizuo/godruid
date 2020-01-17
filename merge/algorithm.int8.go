package merge

func sumInt8(vals ...interface{}) int8 {
	var ret int8
	for _, v := range vals {
		ret += v.(int8)
	}
	return ret
}

func subInt8(vals ...interface{}) int8 {
	var ret = vals[0].(int8)
	for _, v := range vals[1:] {
		ret -= v.(int8)
	}
	return ret
}

func multiplyInt8(vals ...interface{}) int64 {
	if len(vals) == 0 {
		return 0
	}

	var ret int64 = 1
	for _, v := range vals {
		ret *= int64(v.(int8))
	}
	return ret
}

func divideInt8(vals ...interface{}) float32 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = float32(vals[0].(int8))
	for _, v := range vals[1:] {
		vv := v.(int8)
		if vv == 0 {
			return 0
		}
		ret /= float32(vv)
	}
	return ret
}

func minInt8(vals ...interface{}) int8 {
	var ret int8
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int8)
	for _, v := range vals[1:] {
		vv := v.(int8)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}
func maxInt8(vals ...interface{}) int8 {
	var ret int8
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int8)
	for _, v := range vals[1:] {
		vv := v.(int8)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
