package godruid

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	cache "github.com/patrickmn/go-cache"
)

const (
	DefaultEndPoint = "/druid/v2"
)

// MemCache 内存存储
type MemCache cache.Cache

type Client struct {
	Url          string
	EndPoint     string
	DataSource   string
	Debug        bool
	LastRequest  string
	LastResponse string
	HttpClient   *http.Client
	ResultCache  *MemCache
}

// dataKey create a md5sum key for a given data
func dataKey(data []byte) string {
	var tmpData interface{}
	json.Unmarshal(data, &tmpData)
	sortedBytes, _ := json.Marshal(tmpData)
	return fmt.Sprintf("%x", md5.Sum(sortedBytes))
}

func (c *Client) Query(query Query, authToken string) (err error) {
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
		cachedVal, ok := c.ResultCache.Get(qKey)
		if ok {
			result = cachedVal.([]byte)
		} else {
			result, err = c.QueryRaw(reqJson, authToken)
			if err != nil {
				return
			}
			c.ResultCache.Set(qKey, result, 0)
		}
	} else {
		result, err = c.QueryRaw(reqJson, authToken)
		if err != nil {
			return
		}
	}

	return query.onResponse(result)
}

func (c *Client) QueryRaw(req []byte, authToken string) (result []byte, err error) {
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
	if authToken != "" {
		cookie := &http.Cookie{
			Name:  "skylight-aaa",
			Value: authToken,
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
