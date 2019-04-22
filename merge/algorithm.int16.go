package merge

func sumInt16(vals ...interface{}) int16 {
	var ret int16
	for _, v := range vals {
		ret += v.(int16)
	}
	return ret
}

func subInt16(vals ...interface{}) int16 {
	var ret = vals[0].(int16)
	for _, v := range vals[1:] {
		ret -= v.(int16)
	}
	return ret
}

func multiplyInt16(vals ...interface{}) int64 {
	if len(vals) == 0 {
		return 0
	}

	var ret int64 = 1
	for _, v := range vals {
		ret *= int64(v.(int16))
	}
	return ret
}

func divideInt16(vals ...interface{}) float32 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = float32(vals[0].(int16))
	for _, v := range vals[1:] {
		vv := v.(int16)
		if vv == 0 {
			return 0
		}
		ret /= float32(vv)
	}
	return ret
}

func minInt16(vals ...interface{}) int16 {
	var ret int16
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int16)
	for _, v := range vals[1:] {
		vv := v.(int16)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}
func maxInt16(vals ...interface{}) int16 {
	var ret int16
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int16)
	for _, v := range vals[1:] {
		vv := v.(int16)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
