package godruid

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"strconv"
)

// PersistenceRow  for persistence StaticTable's row
type PersistenceRow struct {
	TimePos      int64         `json:"timePos"`      // Primary Key, example: 15555555555555
	TimeLen      int64         `json:"timeLen"`      // Primary Key, example: 3600(hour)
	GroupDims    []string      `json:"groupDims"`    // Primary Key, example: OS,AppVersion,AppBuildNum
	AggNames     []string      `json:"aggNames"`     // Primary Key, example: total_file_size, total_time_cost
	PostAggNames []string      `json:"postAggNames"` // Primary Key, example: avg_speed, avg_file_size
	GroupDimVals []string      `json:"groupDimVals"` // Primary Key, json array
	AggTypes     []string      `json:"aggTypes"`     // Value, json array
	PostAggExps  []string      `json:"postAggExps"`  // Value, json map
	AggVals      []interface{} `json:"aggVals"`      // Value, json map
	PostAggVals  []interface{} `json:"postAggVals"`  // Value, json map
	FilterMD5    string 	   `json:"filterMD5"`
}

// DistributeQuery split intervals to whole days and hours
func (q *QueryGroupBy) DistributeQuery() (QueryGroupBy, error) {
	newQ := *q
	intervals := []string{}
	intervalSlots, err := q.DistributeIntervalSlots()
	if err != nil {
		return newQ, err
	}
	for _, intervalSlot := range intervalSlots {
		intervals = append(intervals, intervalSlot.ToInterval())
	}
	newQ.Intervals = intervals
	return newQ, nil
}

// PersistenceRows convert query result to persistence rows
func (q *QueryGroupBy) PersistenceRows() ([]PersistenceRow, error) {
	ret := []PersistenceRow{}
	// ! only for granularities `all`
	if q.Granularity != GranAll {
		return ret, errors.New("TODO:now only support granularity `all`'s result merge")
	}

	if len(q.Intervals) != 1 {
		return ret, fmt.Errorf("only support when intervals has only one interval")
	}
	intervalSlot, iErr := ParseInterval(q.Intervals[0])
	if iErr != nil {
		return ret, iErr
	}
	timePos := 	  intervalSlot.TimePos.Unix()
	timeLen := 	  intervalSlot.TimeLen
	groupDims := q.DimNames()
	aggNames := q.AggNames()
	postAggNames := q.PostAggNames()
	var filterMD5 string
	if q.Filter != nil {
		bs, _ := json.Marshal(q.Filter)
		filterMD5 = dataKey(bs)
	}

	for _, item := range q.QueryResult {
		groupDimVals := []string{}
		aggVals := []interface{}{}
		postAggVals := []interface{}{}
		for _, k := range groupDims {
			v, _ := item.Event[k]
			if v == nil {
				groupDimVals = append(groupDimVals, "")
			} else {
				groupDimVals = append(groupDimVals, v.(string))
			}
		}
		for _, k := range aggNames {
			aggVals = append(aggVals, item.Event[k])
		}
		for _, kp := range postAggNames {
			postAggVals = append(postAggVals, item.Event[kp])
		}
		ret = append(ret, PersistenceRow{
			TimePos: 	  timePos,
			TimeLen: 	  timeLen,
			GroupDims:    groupDims,
			AggNames:     aggNames,
			PostAggNames: postAggNames,
			AggTypes:     q.aggTypes(),
			PostAggExps:  q.postAggExps(),
			GroupDimVals: groupDimVals,
			AggVals:      aggVals,
			PostAggVals:  postAggVals,
			FilterMD5: 	  filterMD5,
		})
	}

	return ret, nil
}

// LoadQueryResult Load Query Result From Cache maps
func (q *QueryGroupBy) LoadQueryResult(pRows []PersistenceRow) error {
	var queryResult []GroupbyItem
	for _, row := range pRows {
		// TODO: 暂时不管row中的 TimePos及 TimeLen, GroupDims，AggNames，PostAggNames 后续完善时是需要校验的
		event := map[string]interface{}{}
		for i, d := range row.GroupDims {
			event[d] = row.GroupDimVals[i]
		}
		for i, k := range row.AggNames {
			event[k] = row.AggVals[i]
		}
		for i, k := range row.PostAggNames {
			event[k] = row.PostAggVals[i]
		}
		queryResult = append(queryResult, GroupbyItem{Event: event})
	}
	q.QueryResult = queryResult
	return nil
}

// CacheQuery query with attached cached
func (q *QueryGroupBy) CacheQuery(c *Client, target string, writeback bool) error {
	if c.GroupByCache == nil || target == "" {
		return c.Query(q)
	}

	q.setup()
	setDataSource(q, c.DataSource)

	c3 := q.conditionGroupDims()
	c4 := q.conditionAggNames()
	c5 := q.conditionPostAggNames()
	c6 := q.conditionFilterMD5()

	intervalSlots, err := q.DistributeIntervalSlots()
	if err != nil {
		return err
	}

	for _, i := range intervalSlots {
		selectConditions := []Condition{q.conditionTimePos(i.TimePos), q.conditionTimeLen(i.TimeLen), c3, c4, c5, c6}
		cacheSelectQuery := CacheSelectQuery{Target: target, Conditions: selectConditions}
		newQ := *q
		newQ.Intervals = []string{i.ToInterval()}
		// * 如果查询成功,则将结果merge到queryResult中。查询失败则调用原始查询函数进行查询
		ret := c.GroupByCache.Select(cacheSelectQuery)
		if len(ret) > 0 {
			newQ.setup()
			setDataSource(&newQ, c.DataSource)
			newQ.LoadQueryResult(ret)
		} else {
			c.logger().Debugf("[%s] no entries cached by index:%v", "QueryGroupBy.CacheQuery", target)
			err := c.Query(&newQ)
			if err != nil {
				return err
			}
			if writeback {
				rows, _ := newQ.PersistenceRows()
				c.logger().Debugf("[%s] save query result to cache by index:%v, count:%d", "QueryGroupBy.CacheQuery", target, len(rows))
				err := c.GroupByCache.InsertBatch(target, rows, 0)

				if err != nil {
					return err
				}
			}
		}
		if err := q.Merge(&newQ); err != nil {
			return err
		}
	}

	return nil
}

func (q *QueryGroupBy) conditionTimePos(t time.Time) Condition {
	return Condition{FieldName: "timePos", Op: "=", Value: strconv.FormatInt(t.Unix(), 10)}
}

func (q *QueryGroupBy) conditionTimeLen(timeLen int64) Condition {
	return Condition{FieldName: "timeLen", Op: "=", Value: strconv.FormatInt(timeLen, 10)}
}

func (q *QueryGroupBy) conditionGroupDims() Condition {
	return Condition{FieldName: "groupDims", Op: "=", Value: jsonStr(q.DimNames())}
}

func (q *QueryGroupBy) conditionAggNames() Condition {
	return Condition{FieldName: "aggNames", Op: "=", Value: jsonStr(q.AggNames())}
}

func (q *QueryGroupBy) conditionAggTypes() Condition {
	return Condition{FieldName: "aggTypes", Op: "=", Value: jsonStr(q.aggTypes())}
}

func (q *QueryGroupBy) conditionPostAggNames() Condition {
	return Condition{FieldName: "postAggNames", Op: "=", Value: jsonStr(q.PostAggNames())}
}

func (q *QueryGroupBy) conditionPostAggExps() Condition {
	return Condition{FieldName: "postAggExps", Op: "=", Value: jsonStr(q.postAggExps())}
}

func (q *QueryGroupBy) conditionFilterMD5() Condition {
	var filterMD5 string
	if q.Filter != nil {
		bs, _ := json.Marshal(q.Filter)
		filterMD5 = dataKey(bs)
	}
	return Condition{FieldName: "filterMD5", Op: "=", Value: filterMD5}
}

// QueryGroupBy special query for GroupBy type query
func (c *Client) QueryGroupBy(query *QueryGroupBy, cacheIndex string, writeback bool) error {
	c.logger().Debugf("[%s] starting query for `groupBy` query...", "Client.QueryGroupBy")
	if c.GroupByCache == nil || cacheIndex == "" {
		c.logger().Debugf("[%s] no GroupByCache or cacheIndex given", "Client.QueryGroupBy")
		return c.Query(query)
	}
	c.logger().Debugf("[%s] try quering from cache by index:%v", "Client.QueryGroupBy", cacheIndex)
	return query.CacheQuery(c, cacheIndex, writeback)
}

func jsonStr(data interface{}) string {
	b, _ := json.Marshal(data)
	return string(b)
}
