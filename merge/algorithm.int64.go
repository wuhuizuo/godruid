package merge

func sumInt64(vals ...interface{}) int64 {
	var ret int64
	for _, v := range vals {
		ret += v.(int64)
	}
	return ret
}
func subInt64(vals ...interface{}) int64 {
	var ret = vals[0].(int64)
	for _, v := range vals[1:] {
		ret -= v.(int64)
	}
	return ret
}

func multiplyInt64(vals ...interface{}) int64 {
	if len(vals) == 0 {
		return 0
	}

	var ret int64 = 1
	for _, v := range vals {
		ret *= v.(int64)
	}
	return ret
}

func divideInt64(vals ...interface{}) float64 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = float64(vals[0].(int64))
	for _, v := range vals[1:] {
		vv := v.(int64)
		if vv == 0 {
			return 0
		}
		ret /= float64(vv)
	}
	return ret
}

func minInt64(vals ...interface{}) int64 {
	var ret int64
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int64)
	for _, v := range vals[1:] {
		vv := v.(int64)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}

func maxInt64(vals ...interface{}) int64 {
	var ret int64
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int64)
	for _, v := range vals[1:] {
		vv := v.(int64)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
