package godruid

type Filter struct {
	Type        string      `json:"type"`
	Dimension   string      `json:"dimension,omitempty"`
	Value       interface{} `json:"value,omitempty"`
	Pattern     string      `json:"pattern,omitempty"`
	Function    string      `json:"function,omitempty"`
	Field       *Filter     `json:"field,omitempty"`
	Fields      []*Filter   `json:"fields,omitempty"`
	Upper       string      `json:"upper,omitempty"`
	Lower       string      `json:"lower,omitempty"`
	Ordering    string      `json:"ordering,omitempty"`
	UpperStrict bool        `json:"upperStrict,omitempty"`
	LowerStrict bool        `json:"lowerStrict,omitempty"`
}

func FilterSelector(dimension string, value interface{}) *Filter {
	return &Filter{
		Type:      "selector",
		Dimension: dimension,
		Value:     value,
	}
}

func FilterUpperBound(dimension string, ordering string, bound string, strict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Upper:       bound,
		UpperStrict: strict,
	}
}

func FilterLowerBound(dimension string, ordering string, bound string, strict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Lower:       bound,
		LowerStrict: strict,
	}
}

func FilterLowerUpperBound(dimension string, ordering string, lowerBound string, lowerStrict bool, upperBound string, upperStrict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Lower:       lowerBound,
		LowerStrict: lowerStrict,
		Upper:       upperBound,
		UpperStrict: upperStrict,
	}
}

func FilterRegex(dimension, pattern string) *Filter {
	return &Filter{
		Type:      "regex",
		Dimension: dimension,
		Pattern:   pattern,
	}
}

func FilterJavaScript(dimension, function string) *Filter {
	return &Filter{
		Type:      "javascript",
		Dimension: dimension,
		Function:  function,
	}
}

func FilterAnd(filters ...*Filter) *Filter {
	return joinFilters(filters, "and")
}

func FilterOr(filters ...*Filter) *Filter {
	return joinFilters(filters, "or")
}

func FilterNot(filter *Filter) *Filter {
	return &Filter{
		Type:  "not",
		Field: filter,
	}
}

func joinFilters(filters []*Filter, connector string) *Filter {
	// Remove null filters.
	p := 0
	for _, f := range filters {
		if f != nil {
			filters[p] = f
			p++
		}
	}
	filters = filters[0:p]

	fLen := len(filters)
	if fLen == 0 {
		return nil
	}
	if fLen == 1 {
		return filters[0]
	}

	return &Filter{
		Type:   connector,
		Fields: filters,
	}
}
