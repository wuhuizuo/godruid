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

var bearerId = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjhkYmYwYjVkNjcwMTYxOTIyMDIxNDkyOTg3ZGZiOTNjM2FkYWYzMTcifQ.eyJhenAiOiI2Mjc2MzQ4Nzc3NjEtanNndDU2YjA4ODc1YWhkZG43MmRtaXBmcnA4NDhvdTQuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiI2Mjc2MzQ4Nzc3NjEtZnVpdWhtbDI5Y2U3OTg1dWE1cmNqbTJzM2Fkazc5N3YuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTQyOTMwODk5Mjc0MDUzNjM0NDUiLCJoZCI6ImFjY2VkaWFuLmNvbSIsImVtYWlsIjoicHR6b2xvdkBhY2NlZGlhbi5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiYXRfaGFzaCI6Ik0xc2ZITkJvNEtxbEZiaUsxQ0luZlEiLCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE1MTE1NTc3OTIsImV4cCI6MTUxMTU2MTM5Mn0.nl-AkjPCuzy96E8otxHZstekGo8lEV93ZTfKSOcsPyZG6FpokENgL68ta9pp2hYzgglzXO10zs6Z-sAXsi4aaHsW7xnZmq3rmMN7EKhU9PDPu5G3209_F5bVOOcXE-OoRK9k_el8-R4WVBez9itOYPYt8qglB8g99aSBoONUsI_j1Tvq9_EQmOdIJcCcMl4PVM1uoYqsdpyAiFBYUsbrNVqnRUtOdYopqMrrzNlZH5asWcyavWgi8ue4rkyqVF5teIp3AcBsKcR7JTvUI8oX_YX0pFdOAWnYuhoEwOS2sMfJmhOWIPQDupG_0a44gGIwO5_81wqGerWn_B2kPZWGNw"

type Client struct {
	Url      string
	EndPoint string
	Timeout  time.Duration

	Debug        bool
	LastRequest  string
	LastResponse string
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
	result, err := c.QueryRaw(reqJson)
	if err != nil {
		return
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
	request.Header.Set("Authorization", "Bearer "+bearerId)

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
