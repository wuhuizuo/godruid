package merge

func sumFloat64(vals ...interface{}) float64 {
	var ret float64
	for _, v := range vals {
		ret += v.(float64)
	}
	return ret
}

func subFloat64(vals ...interface{}) float64 {
	var ret = vals[0].(float64)
	for _, v := range vals[1:] {
		ret -= v.(float64)
	}
	return ret
}

func multiplyFloat64(vals ...interface{}) float64 {
	if len(vals) == 0 {
		return 0
	}

	var ret float64 = 1
	for _, v := range vals {
		ret *= v.(float64)
	}
	return ret
}

func divideFloat64(vals ...interface{}) float64 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = vals[0].(float64)
	for _, v := range vals[1:] {
		vv := v.(float64)
		if vv == 0 {
			return 0
		}
		ret /= vv
	}
	return ret
}

func minFloat64(vals ...interface{}) float64 {
	var ret float64
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(float64)
	for _, v := range vals[1:] {
		vv := v.(float64)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}

func maxFloat64(vals ...interface{}) float64 {
	var ret float64
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(float64)
	for _, v := range vals[1:] {
		vv := v.(float64)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
