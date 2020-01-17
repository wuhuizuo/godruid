package merge

func sumInt(vals ...interface{}) int {
	var ret int
	for _, v := range vals {
		ret += v.(int)
	}
	return ret
}

func subInt(vals ...interface{}) int {
	var ret = vals[0].(int)
	for _, v := range vals[1:] {
		ret -= v.(int)
	}
	return ret
}

func multiplyInt(vals ...interface{}) int64 {
	if len(vals) == 0 {
		return 0
	}

	var ret int64 = 1
	for _, v := range vals {
		ret *= int64(v.(int))
	}
	return ret
}

func divideInt(vals ...interface{}) float64 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = float64(vals[0].(int))
	for _, v := range vals[1:] {
		vv := v.(int)
		if vv == 0 {
			return 0
		}
		ret /= float64(vv)
	}
	return ret
}

func minInt(vals ...interface{}) int {
	var ret int
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int)
	for _, v := range vals[1:] {
		vv := v.(int)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}

func maxInt(vals ...interface{}) int {
	var ret int
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(int)
	for _, v := range vals[1:] {
		vv := v.(int)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
