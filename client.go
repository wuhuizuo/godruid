package godruid

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// DefaultEndPoint druid api path's end point
const DefaultEndPoint = "/druid/v2"

// CacheThresholdLower the lower threshold for result bytes length
const CacheThresholdLower = 3


// Condition cache query condition
// * copy from https://git.code.oa.com/flarezuo/miglib/blob/master/dcache/jce/DCache/Condition.go
type Condition struct {
	FieldName string `json:"fieldName"`
	Op        string `json:"op"`
	Value     string `json:"value"`
}

type CacheSelectQuery struct {
	Target     string      `json:"target"`
	Conditions []Condition `json:"conditions,omitempty"`
	Fields     []string    `json:"fields,omitempty"`
}

// CacheAdapter interface for druid query result cache middleware client.
type CacheAdapter interface {
	// Get retrieves the cached data by a given key. It also
	// returns true or false, whether it exists or not.
	Get(key string) ([]byte, bool)

	Set(key string, data []byte, lifespan time.Duration)

	// Release frees cache for a given key.
	Release(key string)
}

// GroupByCacheAdapter interface for druid group query result cache middleware client.
type GroupByCacheAdapter interface {
	// Select entries from cache
	Select(query CacheSelectQuery) []map[string]string

	// Insert entry into cache
	Insert(target string, entry map[string]string, lifespan time.Duration) error

	// InsertBatch batch insert entries into cache
	InsertBatch(target string, entries []map[string]string, lifespan time.Duration) error

	// delete entries from cache filter by conditions
	Delete(query CacheSelectQuery) error

	// Clean all entries at given target.
	Clean(target string) error
}

type Client struct {
	Url          string
	EndPoint     string
	DataSource   string
	AuthToken    string
	Debug        bool
	LastRequest  string
	LastResponse string
	HttpClient   *http.Client
	ResultCache  CacheAdapter
	GroupByCache GroupByCacheAdapter
}

// dataKey create a md5sum key for a given data
func dataKey(data []byte) string {
	var tmpData interface{}
	json.Unmarshal(data, &tmpData)
	sortedBytes, _ := json.Marshal(tmpData)
	return fmt.Sprintf("%x", md5.Sum(sortedBytes))
}

func setDataSource(query Query, ds string) error {
	switch query.(type) {
	case *QueryGroupBy:
		a := query.(*QueryGroupBy)
		a.DataSource = ds
	case *QueryScan:
		a := query.(*QueryScan)
		a.DataSource = ds
	case *QuerySearch:
		a := query.(*QuerySearch)
		a.DataSource = ds
	case *QuerySelect:
		a := query.(*QuerySelect)
		a.DataSource = ds
	case *QuerySegmentMetadata:
		a := query.(*QuerySegmentMetadata)
		a.DataSource = ds
	case *QueryTimeBoundary:
		a := query.(*QueryTimeBoundary)
		a.DataSource = ds
	case *QueryTimeseries:
		a := query.(*QueryTimeseries)
		a.DataSource = ds
	case *QueryTopN:
		a := query.(*QueryTopN)
		a.DataSource = ds
	default:
		return fmt.Errorf("not support type: %v", query)
	}

	return nil
}

func (c *Client) Query(query Query) (err error) {
	query.setup()
	setDataSource(query, c.DataSource)
	var reqJson []byte
	if c.Debug {
		reqJson, err = json.MarshalIndent(query, "", "  ")
	} else {
		reqJson, err = json.Marshal(query)
	}
	if err != nil {
		return
	}

	var result []byte
	needCache := (c.ResultCache != nil && query.shouldCache())
	if needCache {
		qKey := dataKey(reqJson)
		var cached bool
		result, cached = c.ResultCache.Get(qKey)
		if !cached || len(result) < CacheThresholdLower {
			result, err = c.QueryRaw(reqJson)
			if err != nil {
				return
			}
			if len(result) >= CacheThresholdLower {
				c.ResultCache.Set(qKey, result, 0)
			}
		}
	} else {
		result, err = c.QueryRaw(reqJson)
		if err != nil {
			return
		}
	}

	return query.onResponse(result)
}

func (c *Client) QueryRaw(req []byte) (result []byte, err error) {
	if c.EndPoint == "" {
		c.EndPoint = DefaultEndPoint
	}
	endPoint := c.EndPoint
	if c.Debug {
		endPoint += "?pretty"
		c.LastRequest = string(req)
	}
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", c.Url+endPoint, bytes.NewBuffer(req))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		cookie := &http.Cookie{
			Name:  "skylight-aaa",
			Value: c.AuthToken,
		}
		request.AddCookie(cookie)
	}

	resp, err := c.HttpClient.Do(request)
	defer func() {
		resp.Body.Close()
	}()

	if err != nil {
		return nil, err
	}

	result, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if c.Debug {
		c.LastResponse = string(result)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(result))
	}

	return
}
