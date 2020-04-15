package godruid

import (
	"fmt"
)

type Filter struct {
	Type         string        `json:"type"`
	Dimension    string        `json:"dimension,omitempty"`
	Value        interface{}   `json:"value,omitempty"`
	Values       interface{}   `json:"values,omitempty"`
	Pattern      string        `json:"pattern,omitempty"`
	Function     string        `json:"function,omitempty"`
	Field        *Filter       `json:"field,omitempty"`
	Fields       []*Filter     `json:"fields,omitempty"`
	Upper        *float64      `json:"upper,omitempty"`
	Lower        *float64      `json:"lower,omitempty"`
	Ordering     Ordering      `json:"ordering,omitempty"`
	UpperStrict  bool          `json:"upperStrict,omitempty"`
	LowerStrict  bool          `json:"lowerStrict,omitempty"`
	ExtractionFn *ExtractionFn `json:"extractionFn,omitempty"`
	Bound        *Bound        `json:"bound,omitempty"`
}

type Bound struct {
	Type      string    `json:"type"`
	MinCoords []float64 `json:"minCoords,omitempty"`
	MaxCoords []float64 `json:"maxCoords,omitempty"`
	Coords    []float64 `json:"coords,omitempty"`
	Radius    float64   `json:"radius,omitempty"`
}

// ToConditions translate to Conditions for db query
func (f Filter) ToConditions() ([]Condition, error) {
	var result []Condition
	switch f.Type {
	case "selector":
		result = append(result, Condition{FieldName: f.Dimension, Op: ConditionOpEql2, Value: f.Value})
	case "not":
		mirrorConditions, err := f.Field.ToConditions()
		if err != nil {
			return result, err
		}
		if len(mirrorConditions) > 1 {
			return result, fmt.Errorf("can not parse when using not logic whth complex filter(translated condition's length > 1)")
		}
		condition := mirrorConditions[0]
		reverseMap := map[string]string{
			ConditionOpEql:    ConditionOpNotEql,
			ConditionOpEql2:   ConditionOpNotEql,
			ConditionOpGT:     ConditionOpLET,
			ConditionOpGET:    ConditionOpLT,
			ConditionOpLT:     ConditionOpGET,
			ConditionOpLET:    ConditionOpGT,
			ConditionOpNotEql: ConditionOpEql,
		}
		reverseOp, ok := reverseMap[condition.Op]
		if !ok {
			return result, fmt.Errorf("can not reverse for Op:%s", condition.Op)
		}
		condition.Op = reverseOp
		result = append(result, condition)
	case "and":
		for _, subF := range f.Fields {
			subCondtions, err := subF.ToConditions()
			if err != nil {
				return result, err
			}
			for _, c := range subCondtions {
				result = append(result, c)
			}
		}
	case "bound":
		if f.Lower != nil {
			condition := Condition{FieldName: f.Dimension, Value: *f.Lower, Op: ">="}
			if f.LowerStrict {
				condition.Op = ConditionOpGT
			}
			result = append(result, condition)
		}
		if f.Upper != nil {
			condition := Condition{FieldName: f.Dimension, Value: *f.Upper, Op: ConditionOpLET}
			if f.UpperStrict {
				condition.Op = ConditionOpLT
			}
			result = append(result, condition)
		}
	default:
		return result, fmt.Errorf("not support filter type: %s", f.Type)

	}

	return result, nil
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

type SpatialCoordinates struct {
	Latitude  float64
	Longitude float64
}

func FilterSpatialRectangle(dimension string, minCoords SpatialCoordinates, maxCoords SpatialCoordinates) *Filter {
	return &Filter{
		Type:      "spatial",
		Dimension: dimension,
		Bound: &Bound{
			Type:      "rectangular",
			MinCoords: []float64{minCoords.Latitude, minCoords.Longitude},
			MaxCoords: []float64{maxCoords.Latitude, maxCoords.Longitude},
		},
	}
}

func FilterSpatialRadius(dimension string, coords SpatialCoordinates, radius float64) *Filter {
	return &Filter{
		Type:      "spatial",
		Dimension: dimension,
		Bound: &Bound{
			Type:   "radius",
			Coords: []float64{coords.Latitude, coords.Longitude},
			Radius: radius,
		},
	}
}

func FilterSelector(dimension string, value interface{}) *Filter {
	return &Filter{
		Type:      "selector",
		Dimension: dimension,
		Value:     value,
	}
}

func FilterUpperBound(dimension string, ordering Ordering, bound float64, strict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Upper:       &bound,
		UpperStrict: strict,
	}
}

func FilterLowerBound(dimension string, ordering Ordering, bound float64, strict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Lower:       &bound,
		LowerStrict: strict,
	}
}

func FilterLowerUpperBound(dimension string, ordering Ordering, lowerBound float64, lowerStrict bool, upperBound float64, upperStrict bool) *Filter {
	return &Filter{
		Type:        "bound",
		Dimension:   dimension,
		Ordering:    ordering,
		Lower:       &lowerBound,
		LowerStrict: lowerStrict,
		Upper:       &upperBound,
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
