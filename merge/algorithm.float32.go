package merge

func sumFloat32(vals ...interface{}) float32 {
	var ret float32
	for _, v := range vals {
		ret += v.(float32)
	}
	return ret
}

func subFloat32(vals ...interface{}) float32 {
	var ret = vals[0].(float32)
	for _, v := range vals[1:] {
		ret -= v.(float32)
	}
	return ret
}

func multiplyFloat32(vals ...interface{}) float64 {
	if len(vals) == 0 {
		return 0
	}

	var ret float64 = 1
	for _, v := range vals {
		ret *= float64(v.(float32))
	}
	return ret
}

func divideFloat32(vals ...interface{}) float32 {
	if len(vals) < 2 {
		panic("argments not enough")
	}

	var ret = vals[0].(float32)
	for _, v := range vals[1:] {
		vv := v.(float32)
		if vv == 0 {
			return 0
		}
		ret /= vv
	}
	return ret
}

func minFloat32(vals ...interface{}) float32 {
	var ret float32
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(float32)
	for _, v := range vals[1:] {
		vv := v.(float32)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}

func maxFloat32(vals ...interface{}) float32 {
	var ret float32
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(float32)
	for _, v := range vals[1:] {
		vv := v.(float32)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
