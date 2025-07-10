//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/discoverer"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/logger"
)

type HTTPClient struct {
	client           *http.Client
	endpoints        []discoverer.Endpoint
	mutex            sync.RWMutex
	groupID          string
	timeout          time.Duration
	maxRetries       int
	log              logger.Logger
	metrics          *metrics
	endpointSelector atomic.Uint64
}

// NewHTTPClient Create a new HTTP client with the given options
func NewHTTPClient(options *Options, log logger.Logger, metrics *metrics) *HTTPClient {
	transport := &http.Transport{
		MaxIdleConns:        options.HTTPMaxConns,
		MaxIdleConnsPerHost: options.HTTPMaxConns / 4,
		IdleConnTimeout:     30 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   options.ConnTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		DisableKeepAlives:  false,
		DisableCompression: false,
	}

	return &HTTPClient{
		client: &http.Client{
			Transport: transport,
			Timeout:   options.HTTPTimeout,
		},
		groupID:    options.GroupID,
		timeout:    options.HTTPTimeout,
		maxRetries: options.MaxRetries,
		log:        log,
		metrics:    metrics,
		endpoints:  make([]discoverer.Endpoint, 0),
	}
}

// SendBatch Send a batch of messages to the DataProxy server
func (c *HTTPClient) SendBatch(ctx context.Context, batch *HTTPBatchReq) error {
	endpoints := c.getEndpoints()
	if len(endpoints) == 0 {
		return fmt.Errorf("no available endpoints")
	}

	// select an endpoint using a simple round-robin strategy
	endpoint := c.selectEndpoint(endpoints)

	req, err := c.buildRequest(ctx, endpoint, batch)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	return c.handleResponse(resp, batch)
}

// buildRequest builds the HTTP request for sending a batch of messages
func (c *HTTPClient) buildRequest(ctx context.Context, endpoint discoverer.Endpoint, batch *HTTPBatchReq) (*http.Request, error) {
	requestURL := fmt.Sprintf("http://%s/dataproxy/message", endpoint.Addr)

	params := url.Values{}
	params.Set("groupId", batch.GroupID)
	params.Set("streamId", batch.StreamID)
	params.Set("dt", strconv.FormatInt(batch.DataTime, 10))
	params.Set("body", strings.Join(batch.Bodies, "\n"))
	params.Set("cnt", strconv.Itoa(len(batch.Bodies)))

	req, err := http.NewRequestWithContext(ctx, "POST", requestURL,
		strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Connection", "keep-alive")

	return req, nil
}

// handleResponse handles the HTTP response from the DataProxy server
func (c *HTTPClient) handleResponse(resp *http.Response, batch *HTTPBatchReq) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body failed: %w", err)
	}

	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		if code, parseErr := strconv.Atoi(strings.TrimSpace(string(body))); parseErr == nil {
			if code == 0 {
				return nil
			}
			return fmt.Errorf("send failed with code: %d", code)
		}
		return fmt.Errorf("parse response failed: %w", err)
	}

	if result.Code == 0 {
		return nil
	}

	return fmt.Errorf("send failed: %s (code: %d)", result.Msg, result.Code)
}

// selectEndpoint selects an endpoint from the available endpoints.
func (c *HTTPClient) selectEndpoint(endpoints []discoverer.Endpoint) discoverer.Endpoint {
	if len(endpoints) == 1 {
		return endpoints[0]
	}

	index := c.endpointSelector.Add(1) % uint64(len(endpoints))
	return endpoints[index]
}

// UpdateEndpoints updates the list of endpoints for the HTTP client.
func (c *HTTPClient) UpdateEndpoints(endpoints []discoverer.Endpoint) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.endpoints = endpoints
	c.log.Infof("HTTP client updated endpoints: %d nodes", len(endpoints))
}

// getEndpoints returns the current list of endpoints.
func (c *HTTPClient) getEndpoints() []discoverer.Endpoint {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.endpoints
}
