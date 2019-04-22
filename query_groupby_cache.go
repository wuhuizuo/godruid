package godruid

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/wuhuizuo/godruid/merge"
)

// PersistenceRow  for persistence StaticTable's row
type PersistenceRow struct {
	TimePos      int32                  `json:"timePos"`      // UKey, example: 15555555555555
	TimeLen      int32                  `json:"timeLen"`      // UKey, example: 3600(hour)
	GroupDims    []string               `json:"groupDims"`    // UKey, example: OS,AppVersion,AppBuildNum
	AggNames     []string               `json:"aggNames"`     // UKey, example: total_file_size, total_time_cost
	PostAggNames []string               `json:"PostAggNames"` // UKey, example: avg_speed, avg_file_size
	GroupDimVals []string               `json:"groupDimVals"` // Value, json array
	AggTypes     []string               `json:"aggTypes"`     // Value, json array
	PostAggExps  []string               `json:"PostAggExps"`  // Value, json map
	AggVals      map[string]interface{} `json:"aggVals"`      // Value, json map
	PostAggVals  map[string]interface{} `json:"postAggVals"`  // Value, json map
}

// Merge 合并聚合
func (q *QueryGroupBy) Merge(oq *QueryGroupBy) error {
	err := q.canMerge(oq)
	if err != nil {
		return err
	}

	q.merge(oq)
	return nil
}

func (q *QueryGroupBy) merge(oq *QueryGroupBy) {
	q.mergeIntervals(oq.Intervals)
	q.mergeQueryResult(oq.QueryResult)
	// 重新生成json
}

func (q *QueryGroupBy) mergeIntervals(intervals Intervals) {
	// TODO: 暂时不实现
}

func (q *QueryGroupBy) mergeEvent(eventA, eventB map[string]interface{}, aggNames, aggTypes []string) map[string]interface{} {
	ret := map[string]interface{}{}
	if len(eventA) == 0 {
		return eventB
	}
	if len(eventB) == 0 {
		return eventA
	}

	for i, aggName := range aggNames {
		ret[aggName] = mergeAgg(aggTypes[i], eventA[aggName], eventB[aggName])
	}

	return ret
}

func mergeAgg(aggType string, aggVals ...interface{}) interface{} {
	if len(aggVals) == 0 {
		return nil
	}
	if len(aggVals) == 1 {
		return aggVals[0]
	}

	switch aggType {
	case "count", "longSum", "doubleSum":
		return merge.Sum(aggVals...)
	case "min", "doubleMin", "longMin":
		return merge.Min(aggVals...)
	case "max", "doubleMax", "longMax":
		return merge.Max(aggVals...)
	default:
		//TODO: 平滑错误
		panic("不支持的合并agg")
	}

	return nil
}

func reComputePostAgg(event map[string]interface{}, postAggName string, postAggExp []string) {
	if v := merge.PostAggComputeArithmetic(event, postAggExp); v != nil {
		event[postAggName] = v
	}
}

func reComputePostAggs(event map[string]interface{}, postAggNames []string, postAggExps [][]string) {
	for i, postAggName := range postAggNames {
		reComputePostAgg(event, postAggName, postAggExps[i])
	}
}

func (q *QueryGroupBy) mergeQueryResult(oResult []GroupbyItem) {
	newResult := []GroupbyItem{}
	var iR, iO int
	resultLen := len(q.QueryResult)
	oResultLen := len(oResult)
	dimNames := q.dimNames()
	aggNames := q.aggNames()
	aggTypes := q.aggTypes()

	// merge aggragation values
	// 采用混合插入有序数组方法
	for iR < resultLen && iO < oResultLen {
		cI, cE := merge.MapCompare(dimNames, q.QueryResult[iR].Event, oResult[iO].Event)
		if cE != nil {
			// TODO: 先panic,后续在平滑的处理
			panic(cE.Error())
		}

		switch cI {
		case merge.CompareEQ:
			if q.eventCanMerge(dimNames, q.QueryResult[iR].Event, oResult[iO].Event) {
				mergedEvent := q.mergeEvent(q.QueryResult[iR].Event, oResult[iO].Event, aggNames, aggTypes)
				q.QueryResult[iR].Event = mergedEvent
				newResult = append(newResult, q.QueryResult[iR])
				iR++
				iO++
			} else {
				// TODO: 先panic,后续在平滑的处理
				panic("数据错误:比较相等,但缺不能合并,可能缺少必要的纬度key")
			}
		case merge.CompareLT:
			newResult = append(newResult, q.QueryResult[iR])
			iR++
		case merge.CompareGT:
			newResult = append(newResult, oResult[iO])
			iO++
		default:
			panic(fmt.Sprintf("comapre ret code is:%d", cI))
		}
	}
	if iR < resultLen {
		for _, item := range q.QueryResult[iR : resultLen-1] {
			newResult = append(newResult, item)
		}
	}
	if iO < oResultLen {
		for _, item := range oResult[iO : oResultLen-1] {
			newResult = append(newResult, item)
		}
	}

	// re compute post aggragation values
	postAggNames := q.postAggNames()
	postAggExps := q.postAggExps()
	for _, item := range newResult {
		reComputePostAggs(item.Event, postAggNames, postAggExps)
	}

	q.QueryResult = newResult
}

func (q *QueryGroupBy) eventCanMerge(dims []string, event1, event2 map[string]interface{}) bool {
	for _, d := range dims {
		v1, ok1 := event1[d]
		v2, ok2 := event2[d]
		if !(ok1 && ok2 && v1 == v2) {
			return false
		}
	}

	return true
}

func (q *QueryGroupBy) dimNames() []string {
	ret := []string{}
	for _, dim := range q.Dimensions {
		switch dim.(type) {
		case string:
			ret = append(ret, dim.(string))
		case Dimension:
			ret = append(ret, dim.(Dimension).OutputName)
		case TimeExtractionDimensionSpec:
			panic("not support for TimeExtractionDimensionSpec")
		}
	}

	return ret
}

func (q *QueryGroupBy) aggNames() []string {
	ret := []string{}
	for _, agg := range q.Aggregations {
		if agg.Type == "filtered" {
			ret = append(ret, agg.Aggregator.Name)
		} else {
			ret = append(ret, agg.Name)
		}
	}
	return ret
}

func aggType(agg Aggregation) string {
	if agg.Type == "filtered" {
		return aggType(*agg.Aggregator)
	}
	return agg.Type
}

func (q *QueryGroupBy) aggTypes() []string {
	ret := []string{}
	for _, agg := range q.Aggregations {
		ret = append(ret, aggType(agg))
	}
	return ret
}

func (q *QueryGroupBy) postAggNames() []string {
	ret := []string{}
	for _, pa := range q.PostAggregations {
		ret = append(ret, pa.Name)
	}
	return ret
}

func (q *QueryGroupBy) postAggExps() [][]string {
	ret := [][]string{}
	for _, pa := range q.PostAggregations {
		ret = append(ret, postAggExp(pa))
	}
	return ret
}

func (q *QueryGroupBy) postAggExpStrings() []string {
	var ret []string
	for _, exp := range q.postAggExps() {
		ret = append(ret, fmt.Sprintf("%s:%s", exp[0], strings.Join(exp[1:], ",")))
	}
	return ret
}

func postAggExp(pg PostAggregation) []string {
	switch pg.Type {
	case "arithmetic":
		ret := []string{pg.Fn}
		for _, f := range pg.Fields {
			ret = append(ret, f.FieldName)
		}
		return ret
	default:
		panic("not support PostAggregation type")
	}
}

func (q *QueryGroupBy) canMerge(oq *QueryGroupBy) error {
	// ! only for granularities `all`
	if q.Granularity != GranAll || oq.Granularity != GranAll {
		return errors.New("TODO:now only support granularity `all`'s result merge")
	}
	if reflect.DeepEqual(q.Intervals, oq.Intervals) {
		return errors.New("can not merge with same intervals")
	}
	if !(q.DataSource == oq.DataSource) {
		return errors.New("DataSource is not same")
	}
	if !reflect.DeepEqual(q.Context, oq.Context) {
		return errors.New("Context is not same")
	}
	if !reflect.DeepEqual(q.VirtualColumns, oq.VirtualColumns) {
		return errors.New("VirtualColumns is not same")
	}
	if !reflect.DeepEqual(q.Dimensions, oq.Dimensions) {
		return errors.New("Dimensions is not same")
	}
	if !reflect.DeepEqual(q.Aggregations, oq.Aggregations) {
		return errors.New("Aggregations is not same")
	}
	if !reflect.DeepEqual(q.PostAggregations, oq.PostAggregations) {
		return errors.New("PostAggregations is not same")
	}

	if q.LimitSpec != nil && oq.LimitSpec != nil {
		if !reflect.DeepEqual(*(q.LimitSpec), *(oq.LimitSpec)) {
			return errors.New("LimitSpec is not same")
		}
	} else if q.LimitSpec != oq.LimitSpec {
		return errors.New("LimitSpec is not same")
	}

	if q.Having != nil && oq.Having != nil {
		if !reflect.DeepEqual(*(q.Having), *(oq.Having)) {
			return errors.New("Having is not same")
		}
	} else if q.Having != oq.Having {
		return errors.New("Having is not same")
	}

	if q.Filter != nil && oq.Filter != nil {
		if !reflect.DeepEqual(*(q.Filter), *(oq.Filter)) {
			return errors.New("Filter is not same")
		}
	} else if q.Filter != oq.Filter {
		return errors.New("Filter is not same")
	}

	return nil
}
