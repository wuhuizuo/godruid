package merge

func minString(vals ...interface{}) string {
	var ret string
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(string)
	for _, v := range vals[1:] {
		vv := v.(string)
		if ret > vv {
			ret = vv
		}
	}
	return ret
}

func maxString(vals ...interface{}) string {
	var ret string
	if len(vals) == 0 {
		return ret
	}
	ret = vals[0].(string)
	for _, v := range vals[1:] {
		vv := v.(string)
		if ret < vv {
			ret = vv
		}
	}
	return ret
}
