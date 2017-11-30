package godruid

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	DefaultEndPoint = "/druid/v2"
)

type Client struct {
	Url      string
	EndPoint string
	Timeout  time.Duration

	Debug        bool
	LastRequest  string
	LastResponse string
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

	fmt.Println("QUERY--->", string(reqJson))

	result, err := c.QueryRaw(reqJson, authToken)
	if err != nil {
		return
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

	// By default, use 60 second timeout unless specified otherwise
	// by the caller
	clientTimeout := 60 * time.Second
	if c.Timeout != 0 {
		clientTimeout = c.Timeout
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	httpClient := &http.Client{
		Timeout:   clientTimeout,
		Transport: tr,
	}

	request, err := http.NewRequest("POST", "https://broker.proto.npav.accedian.net/druid/v2?pretty=", bytes.NewBuffer(req))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := httpClient.Do(request)

	if err != nil {
		return
	}
	defer func() {
		resp.Body.Close()
	}()

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
