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

const (
	DefaultEndPoint = "/druid/v2"
)

// CacheAdapter interface for druid query result cache middleware client.
type CacheAdapter interface {
	// Get retrieves the cached data by a given key. It also
	// returns true or false, whether it exists or not.
	Get(key string) ([]byte, bool)

	Set(key string, data []byte, lifespan time.Duration)

	// Release frees cache for a given key.
	Release(key string)
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
}

// dataKey create a md5sum key for a given data
func dataKey(data []byte) string {
	var tmpData interface{}
	json.Unmarshal(data, &tmpData)
	sortedBytes, _ := json.Marshal(tmpData)
	return fmt.Sprintf("%x", md5.Sum(sortedBytes))
}

func (c *Client) Query(query Query) (err error) {
	query.setup()
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
		if !cached {
			result, err = c.QueryRaw(reqJson)
			if err != nil {
				return
			}
			c.ResultCache.Set(qKey, result, 0)
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
