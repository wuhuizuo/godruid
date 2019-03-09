package godruid

type Filter struct {
	Type         string        `json:"type"`
	Dimension    string        `json:"dimension,omitempty"`
	Value        interface{}   `json:"value,omitempty"`
	Values       interface{}   `json:"values,omitempty"`
	Pattern      string        `json:"pattern,omitempty"`
	Function     string        `json:"function,omitempty"`
	Field        *Filter       `json:"field,omitempty"`
	Fields       []*Filter     `json:"fields,omitempty"`
	Upper        float32       `json:"upper,omitempty"`
	Lower        float32       `json:"lower,omitempty"`
	Ordering     Ordering      `json:"ordering,omitempty"`
	UpperStrict  bool          `json:"upperStrict,omitempty"`
	LowerStrict  bool          `json:"lowerStrict,omitempty"`
	ExtractionFn *ExtractionFn `json:"extractionFn,omitempty"`
}

type Ordering string

const (
	LEXICOGRAPHIC Ordering = "lexicographic"
	ALPHANUMERIC  Ordering = "alphanumeric"
	NUMERIC       Ordering = "numeric"
	STRLEN        Ordering = "strlen"
)

// Filter constants
const (
	LOWERSTRICT = "lowerStrict"
	UPPERSTRICT = "upperStrict"
	LOWERLIMIT  = "lowerLimit"
	UPPERLIMIT  = "upperLimit"
)

func FilterSelector(dimension string, value interface{}) *Filter {
	return &Filter{
		Type:      "selector",
		Dimension: dimension,
		Value:     value,
	}
}

func FilterUpperBound(dimension string, ordering Ordering, bound float32, strict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Upper:       bound,
		UpperStrict: strict,
	}
}

func FilterLowerBound(dimension string, ordering Ordering, bound float32, strict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Lower:       bound,
		LowerStrict: strict,
	}
}

func FilterLowerUpperBound(dimension string, ordering Ordering, lowerBound float32, lowerStrict bool, upperBound float32, upperStrict bool) *Filter {
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

func FilterLike(dimension, pattern string) *Filter {
	return &Filter{
		Type:      "like",
		Dimension: dimension,
		Pattern:   pattern,
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

func flattenFilters(filters []*Filter, connector string) []*Filter {
	fields := []*Filter{}

	// Remove null filters and flatten same type filter with connector.
	for _, f := range filters {
		if f != nil {
			if f.Type == connector {
				for _, f := range flattenFilters(f.Fields, connector) {
					fields = append(fields, f)
				}
			} else {
				fields = append(fields, f)
			}
		}
	}

	return fields
}

func joinFilters(filters []*Filter, connector string) *Filter {
	fields := flattenFilters(filters, connector)

	fLen := len(fields)
	if fLen == 0 {
		return nil
	}
	if fLen == 1 {
		return fields[0]
	}

	return &Filter{
		Type:   connector,
		Fields: fields,
	}
}
