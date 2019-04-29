package merge

func sumInt32(vals ...interface{}) int32 {
	var ret int32
	for _, v := range vals {
		ret += v.(int32)
	}
	return ret
}
func subInt32(vals ...interface{}) int32 {
	var ret = vals[0].(int32)
	for _, v := range vals[1:] {
		ret -= v.(int32)
	}
	return ret
}

func multiplyInt32(vals ...interface{}) int64 {
	if len(vals) == 0 {
		return 0
	}

	var ret int64 = 1
	for _, v := range vals {
		ret *= int64(v.(int32))
	}
	return ret
}

func divideInt32(vals ...interface{}) float32 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = float32(vals[0].(int32))
	for _, v := range vals[1:] {
		vv := v.(int32)
		if vv == 0 {
			return 0
		}
		ret /= float32(vv)
	}
	return ret
}

func minInt32(vals ...interface{}) int32 {
	var ret int32
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int32)
	for _, v := range vals[1:] {
		vv := v.(int32)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}

func maxInt32(vals ...interface{}) int32 {
	var ret int32
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int32)
	for _, v := range vals[1:] {
		vv := v.(int32)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
