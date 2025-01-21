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

package connpool

import (
	"context"
	"errors"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/logger"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/util"

	"github.com/panjf2000/gnet/v2"
)

const (
	defaultConnCloseDelay = 2 * time.Minute
)

// error variables
var (
	ErrInitEndpointEmpty   = errors.New("init endpoints is empty")
	ErrDialerIsNil         = errors.New("dialer is nil")
	ErrLoggerIsNil         = errors.New("logger is nil")
	ErrNoAvailableEndpoint = errors.New("no available server endpoint")
)

// Dialer is the interface of a dialer that return a NetConn
type Dialer interface {
	// Dial dials to the addr and bind ctx to the returned connection, which network(TCP/UDP) to use is determined by the Dialer
	// Dial should use gnet.Client.DialContext() to get a connection that can be driven by a gnet event engine.
	Dial(addr string, ctx any) (gnet.Conn, error)
}

// ConnContext is the additional attributes to set to a gnet.Conn
type ConnContext struct {
	CreatedAt time.Time // the created time of the connection
	Endpoint  string    // the address of the remote endpoint
}

// EndpointRestrictedConnPool is the interface of a simple endpoint restricted connection pool that
// the connection's remote address must be in an endpoint list, if not, it will be closed and can
// not be used anymore, it is useful for holding the connections to a service whose endpoints can
// be changed at runtime.
// Best practice:
// gnet is a high-performance networking package, the best way to use this pool is:
//  1. call Get() to get a gnet.Conn;
//  2. use the conn to read/write for a duration, 1m, for example, and then put the conn back to the pool and get a new one for load balancing, avoid putting/getting frequently;
//  3. do not switch(put and get) to a new conn in the callback of gnet.Conn.AsyncWrite(buf []byte, callback AsyncCallback) or gnet.Conn.AsyncWritev(bs [][]byte, callback AsyncCallback), it may be blocked;
//  4. if you use TCP conn and can not update endpoints by service discovery directly, for example, your endpoints are behind at the back of a LB, it is better to set a max lifetime
//     for your pool, so that you can restart your endpoints(RS) without data lost by:
//     1). set the weight of your endpoint(RS) to 0, so that no new connection incoming;
//     2). wait for the existing connections to close by lifetime timeout;
//     3). restart your endpoint.
type EndpointRestrictedConnPool interface {
	// Get gets a connection, it's concurrency-safe, but you can not call it in the callback of gnet.Conn.AsyncWrite() or gnet.Conn.AsyncWritev().
	Get() (gnet.Conn, error)
	// Put puts a connection back to the pool, if err is not nil, the connection will be closed by the pool, it's concurrency-safe,
	// but you can not call it in the callback of gnet.Conn.AsyncWrite() or gnet.Conn.AsyncWritev().
	Put(conn gnet.Conn, err error)
	// UpdateEndpoints updates the endpoints the pool to dial to, it's not concurrency-safe.
	UpdateEndpoints(all, add, del []string)
	// NumPooled returns the connection number in the pool, not the number of all the connection that the pool created, it's concurrency-safe.
	NumPooled() int
	// OnConnClosed used to notify that a connection is closed, the connection will be removed from the pool, if err is not nil, the remote endpoint will mark as unavailable, it's concurrency-safe.
	OnConnClosed(conn gnet.Conn, err error)
	// Close closes the pool
	Close()
}

// NewConnPool news a EndpointRestrictedConnPool
func NewConnPool(initEndpoints []string, connsPerEndpoint, size int,
	dialer Dialer, log logger.Logger, maxConnLifetime time.Duration) (EndpointRestrictedConnPool, error) {
	if len(initEndpoints) == 0 {
		return nil, ErrInitEndpointEmpty
	}

	if connsPerEndpoint <= 0 {
		connsPerEndpoint = 1
	}

	if dialer == nil {
		return nil, ErrDialerIsNil
	}

	if log == nil {
		return nil, ErrLoggerIsNil
	}

	requiredConnNum := len(initEndpoints) * connsPerEndpoint
	if size <= 0 {
		size = int(math.Max(1024, float64(requiredConnNum)))
	}

	// copy endpoints
	endpoints := make([]string, 0, len(initEndpoints))
	endpoints = append(endpoints, initEndpoints...)

	pool := &connPool{
		connChan:         make(chan gnet.Conn, size),
		connsPerEndpoint: connsPerEndpoint,
		requiredConnNum:  requiredConnNum,
		dialer:           dialer,
		log:              log,
		backoff: util.ExponentialBackoff{
			InitialInterval: 10 * time.Second,
			MaxInterval:     1 * time.Minute,
			Multiplier:      2,
			Randomization:   0.5,
		},
		closeCh:         make(chan struct{}),
		maxConnLifetime: maxConnLifetime,
	}

	// store endpoints
	pool.endpoints.Store(endpoints)

	// store endpoints to map
	for _, e := range endpoints {
		pool.endpointMap.Store(e, struct{}{})
	}

	err := pool.initConns(requiredConnNum)
	if err != nil {
		return nil, err
	}

	// starts a backbround task, do rebalancing and recovering periodically
	go pool.recoverAndRebalance()

	return pool, nil
}

type connPool struct {
	connChan           chan gnet.Conn
	index              atomic.Uint64
	endpoints          atomic.Value
	endpointMap        sync.Map
	connsPerEndpoint   int
	requiredConnNum    int
	dialer             Dialer
	log                logger.Logger
	unavailable        sync.Map
	retryCounts        sync.Map
	backoff            util.ExponentialBackoff
	closeCh            chan struct{}
	closeOnce          sync.Once
	endpointConnCounts sync.Map      // store the conn count of each endpoint
	maxConnLifetime    time.Duration // the max lifetime of a connection
}

func (p *connPool) expired(conn gnet.Conn) bool {
	if conn == nil || p.maxConnLifetime <= 0 {
		return false
	}

	ctx := conn.Context()
	if ctx == nil {
		return false
	}

	connCtx, ok := ctx.(ConnContext)
	if !ok {
		return false
	}
	return connCtx.CreatedAt.Add(p.maxConnLifetime).Before(time.Now())
}

func (p *connPool) Get() (gnet.Conn, error) {
	p.log.Debug("Get()")
	select {
	case conn := <-p.connChan:
		return conn, nil
	default:
		conn, err := p.newConn()
		if err != nil {
			return nil, err
		}
		addr := conn.RemoteAddr()
		if addr == nil {
			CloseConn(conn, 0)
			p.log.Error("new connection has nil remote address")
			return nil, errors.New("new connection has nil remote address")
		}
		p.incEndpointConnCount(addr.String())
		return conn, nil
	}
}

func (p *connPool) getEndpoint() (string, error) {
	p.log.Debug("getEndpoint()")
	epValue := p.endpoints.Load()
	endpoints, ok := epValue.([]string)
	if !ok || len(endpoints) == 0 {
		return "", ErrNoAvailableEndpoint
	}

	for i := 0; i < len(endpoints); i++ {
		index := p.index.Load()
		p.index.Add(1)
		ep := endpoints[index%uint64(len(endpoints))]

		// if endpoint is in the unavailable list, skip it
		_, unavailable := p.unavailable.Load(ep)
		if unavailable {
			continue
		}

		return ep, nil
	}

	return "", ErrNoAvailableEndpoint
}

func (p *connPool) newConn() (gnet.Conn, error) {
	p.log.Debug("newConn()")
	ep, err := p.getEndpoint()
	if err != nil {
		return nil, err
	}

	return p.dialNewConn(ep)
}

func (p *connPool) dialNewConn(ep string) (gnet.Conn, error) {
	p.log.Debug("dialNewConn()")
	conn, err := p.dialer.Dial(ep, ConnContext{CreatedAt: time.Now(), Endpoint: ep})
	if err != nil {
		p.markUnavailable(ep)
		return nil, err
	}
	return conn, nil
}

func (p *connPool) initConns(count int) error {
	// create some conns and then put them back to the pool
	var wg sync.WaitGroup
	conns := make(chan gnet.Conn, count)
	errs := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.newConn()
			if err != nil {
				errs <- err
				return
			}
			conns <- conn
		}()
	}

	wg.Wait()
	close(conns)
	close(errs)

	for err := range errs {
		if err != nil {
			for conn := range conns {
				_ = conn.Close()
			}
			return err
		}
	}

	for conn := range conns {
		p.put(conn, nil, true)
	}

	return nil
}

func (p *connPool) Put(conn gnet.Conn, err error) {
	p.put(conn, err, false)
}

func (p *connPool) put(conn gnet.Conn, err error, isNewConn bool) {
	if conn == nil {
		return
	}

	remoteAddr := conn.RemoteAddr()
	if remoteAddr == nil {
		p.log.Error("remote address is nil, it is closed, stop putting")
		CloseConn(conn, defaultConnCloseDelay)
		return
	}

	addr := remoteAddr.String()
	if _, ok := p.endpointMap.Load(addr); !ok {
		p.log.Warn("endpoint deleted, close its connection, addr:", addr)
		CloseConn(conn, defaultConnCloseDelay)
		return
	}

	// if an error occurs, close it
	if err != nil {
		p.log.Warn("connection error, close it, addr:", addr, ", err:", err)
		CloseConn(conn, defaultConnCloseDelay)
		return
	}

	// if conn is expired, close it
	if p.expired(conn) {
		p.log.Debug("connection expired, close it, addr:", addr, ", err:", err)
		CloseConn(conn, defaultConnCloseDelay)
		// when conn is closed, available connections is less than before, the number of connections for the endpoint may not be balanced,
		// although it may be called recursively, we still append a new connection here
		_ = p.appendNewConn(addr)
		return
	}

	select {
	case p.connChan <- conn:
		// update the conn count
		if isNewConn {
			p.incEndpointConnCount(addr)
		}
	default:
		// connChan is full, close the connection after 2m
		p.log.Warn("connection pool is full, closing connection, addr: ", addr)
		CloseConn(conn, defaultConnCloseDelay)
	}
}

func (p *connPool) incEndpointConnCount(addr string) {
	count, _ := p.endpointConnCounts.LoadOrStore(addr, 0)
	p.endpointConnCounts.Store(addr, count.(int)+1)
}

func (p *connPool) decEndpointConnCount(addr string) {
	count, ok := p.endpointConnCounts.Load(addr)
	if !ok {
		return
	}

	if count.(int) > 0 {
		if count.(int) == 1 {
			p.endpointConnCounts.Delete(addr)
			return
		}

		p.endpointConnCounts.Store(addr, count.(int)-1)
	}
}

func (p *connPool) UpdateEndpoints(all, add, del []string) {
	defer func() {
		if rec := recover(); rec != nil {
			p.log.Error("panic when update endpoints:", rec)
			p.log.Error(string(debug.Stack()))
		}
	}()

	if len(all) == 0 {
		return
	}
	p.log.Debug("UpdateEndpoints")
	p.log.Debug("all:", all)
	p.log.Debug("add:", add)
	p.log.Debug("del:", del)
	endpoints := make([]string, 0, len(all))
	endpoints = append(endpoints, all...)
	p.endpoints.Store(endpoints)

	// store new endpoints to map
	for _, ep := range add {
		p.endpointMap.Store(ep, struct{}{})
	}

	//
	delEndpoints := make(map[string]struct{})
	for _, ep := range del {
		p.endpointMap.Delete(ep)
		p.unavailable.Delete(ep)
		p.retryCounts.Delete(ep)

		delEndpoints[ep] = struct{}{}
	}

	if len(delEndpoints) > 0 {
		// delete connections for deleted endpoints
		p.log.Debug("delete old connections...")

		// use a temp slice to store the conn in connChan
		tempConns := make([]gnet.Conn, 0, cap(p.connChan))
	loop:
		for i := 0; i < cap(p.connChan); i++ {
			select {
			case conn := <-p.connChan:
				// fix: when conn is closed by peer, remote addr may be nil
				remoteAddr := conn.RemoteAddr()
				if remoteAddr == nil {
					CloseConn(conn, 0)
					continue
				}

				addr := remoteAddr.String()
				if _, ok := delEndpoints[addr]; ok {
					p.log.Warn("endpoint deleted, close its connection, addr:", addr)
					CloseConn(conn, defaultConnCloseDelay)
					// for the deleted endpoint, we decrease its conn count before it is really closed, so that we can avoid creating more conns than we expect when rebalance
					p.decEndpointConnCount(addr)
				} else {
					tempConns = append(tempConns, conn)
				}
			default:
				// no more conn, exit the loop
				break loop
			}
		}

		// put the conn back to connChan
		for _, chConn := range tempConns {
			select {
			case p.connChan <- chConn:
			default:
				// if connChan is full, stop putting
				CloseConn(chConn, defaultConnCloseDelay)
			}
		}
	}

	// rebalance
	if len(add) > 0 || len(del) > 0 {
		p.rebalance()
	}
}

func (p *connPool) NumPooled() int {
	return len(p.connChan)
}

// CloseConn closes a connection after a duration of time
func CloseConn(conn gnet.Conn, after time.Duration) {
	if after <= 0 {
		_ = conn.Close()
		return
	}

	ctx := context.Background()
	go func() {
		select {
		case <-time.After(after):
			_ = conn.Close()
			return
		case <-ctx.Done():
			_ = conn.Close()
			return
		}
	}()
}

// OnConnClosed handles conn closed event, call it when conn is closed actively by the server
func (p *connPool) OnConnClosed(conn gnet.Conn, err error) {
	remoteAddr := conn.RemoteAddr()
	if remoteAddr != nil {
		addr := remoteAddr.String()
		if err != nil {
			p.markUnavailable(addr)
		}
		p.decEndpointConnCount(addr)
	}

	// use a temp slice to store the conn in connChan
	tempConns := make([]gnet.Conn, 0, cap(p.connChan))

	// iterate the connChan, find and delete the closed conn
loop:
	for i := 0; i < cap(p.connChan); i++ {
		select {
		case chConn := <-p.connChan:
			if chConn != conn && chConn.RemoteAddr() != nil {
				// if it is not the conn to close, store in the temp slice
				tempConns = append(tempConns, chConn)
			} else {
				if remoteAddr != nil {
					p.log.Debug("remove conn from pool, addr:", remoteAddr.String())
				}
			}
		default:
			// no more conn, exit the loop
			break loop
		}
	}

	// put the conn back to connChan
	for _, chConn := range tempConns {
		select {
		case p.connChan <- chConn:
		default:
			// if connChan is full, stop putting
			CloseConn(chConn, defaultConnCloseDelay)
		}
	}
}

func (p *connPool) markUnavailable(ep string) {
	// if there is only 1 endpoint, it is always available
	// it is common when endpoint address is a CLB VIP
	epCount := p.getEndpointCount()
	if epCount <= 1 {
		return
	}

	p.log.Debug("endpoint cannot be connected, marking as unavailable, addr: ", ep)
	p.unavailable.Store(ep, time.Now())
	p.retryCounts.Store(ep, 0)
}

// recoverAndRebalance recovers the down endpoint and rebalaces the conns periodically
func (p *connPool) recoverAndRebalance() {
	// server failure is a low-probability event, so there's basically no endpoint need to recover, a higher frequency is also acceptable
	recoverTicker := time.NewTicker(10 * time.Second)
	defer recoverTicker.Stop()
	// dump conn pool info every 10s
	dumpTicker := time.NewTicker(10 * time.Second)
	defer dumpTicker.Stop()
	// rebalancing will calculate a new conn count per endpoint based on the total conn count, 'cause our conn is closed after some timeout, so we set the ticker duration bigger than the close time out
	reBalanceTicker := time.NewTicker(defaultConnCloseDelay + 30*time.Second)
	defer reBalanceTicker.Stop()

	// clean expired conn every minute
	var cleanExpiredConnTicker *time.Ticker
	if p.maxConnLifetime > 0 {
		cleanExpiredConnTicker = time.NewTicker(1 * time.Minute)
	}
	defer func() {
		if cleanExpiredConnTicker != nil {
			cleanExpiredConnTicker.Stop()
		}
	}()

	for {
		select {
		case <-recoverTicker.C:
			// rebalace
			recovered := p.recover()
			if recovered {
				p.rebalance()
			}
		case <-dumpTicker.C:
			p.dump()
		case <-reBalanceTicker.C:
			p.rebalance()
		case <-p.closeCh:
			return
		default:
			if cleanExpiredConnTicker != nil {
				select {
				case <-cleanExpiredConnTicker.C:
					p.cleanExpiredConns()
				default:
					time.Sleep(time.Second)
				}
			} else {
				time.Sleep(time.Second)
			}
		}
	}
}

func getRemoteAddr(conn gnet.Conn) string {
	if conn == nil {
		return ""
	}

	addr := conn.RemoteAddr()
	if addr != nil {
		return addr.String()
	}
	ctx := conn.Context()
	if ctx == nil {
		return ""
	}

	connCtx, ok := ctx.(ConnContext)
	if !ok {
		return ""
	}
	return connCtx.Endpoint
}

func (p *connPool) cleanExpiredConns() {
	p.log.Debug("cleanExpiredConns()")
	var leftConns []gnet.Conn
	var expiredConns []gnet.Conn
loop:
	for i := 0; i < cap(p.connChan); i++ {
		select {
		case conn := <-p.connChan:
			if p.expired(conn) {
				expiredConns = append(expiredConns, conn)
				continue
			}

			// not the expired conn, put it back
			leftConns = append(leftConns, conn)
		default:
			// no more conn, exit the loop
			break loop
		}
	}

	// put the conn back to the chan
	for _, left := range leftConns {
		select {
		case p.connChan <- left:
		default:
			CloseConn(left, defaultConnCloseDelay)
		}
	}

	// close the expired conn and append new conn with the same addr
	for _, expired := range expiredConns {
		addr := getRemoteAddr(expired)
		p.log.Debug("connection expired, close it, addr:", addr, ", err:", nil)
		CloseConn(expired, defaultConnCloseDelay)
		_ = p.appendNewConn(addr)
	}
}

func (p *connPool) dump() {
	p.log.Debug("all endpoints:")
	eps := p.endpoints.Load()
	endpoints, ok := eps.([]string)
	if ok {
		for _, ep := range endpoints {
			p.log.Debug(ep)
		}
	}

	dump := false
	p.unavailable.Range(func(key, value any) bool {
		if !dump {
			p.log.Debug("unavailable endpoints:")
		}
		p.log.Debug(key)
		return true
	})

	p.log.Debug("opened connections:")
	p.endpointConnCounts.Range(func(key, value any) bool {
		p.log.Debug("endpoint: ", key, ", conns: ", value.(int))
		return true
	})
}

func (p *connPool) recover() bool {
	recovered := false
	p.unavailable.Range(func(key, value any) bool {
		lastUnavailable := value.(time.Time)
		retries := 0
		if retry, ok := p.retryCounts.Load(key); ok {
			retries = retry.(int)
		}
		if time.Since(lastUnavailable) > p.backoff.Next(retries) {
			// try to create new conn
			conn, err := p.dialer.Dial(key.(string), ConnContext{CreatedAt: time.Now(), Endpoint: key.(string)})
			if err == nil {
				p.log.Debug("endpoint recovered, addr: ", key)
				p.put(conn, nil, true)
				p.unavailable.Delete(key)
				p.retryCounts.Delete(key)
				recovered = true
			} else {
				p.log.Info("failed to recover endpoint, addr: ", key, ", err: ", err)
				// update retry count
				retries++
				p.retryCounts.Store(key, retries)
			}
		}
		return true
	})
	if recovered {
		p.log.Debug("recover triggered")
	}
	return recovered
}

func (p *connPool) getConnCount() int {
	// get the total conn count
	totalConnCount := 0
	p.endpointConnCounts.Range(func(key, value any) bool {
		totalConnCount += value.(int)
		return true
	})
	return totalConnCount
}

func (p *connPool) getEndpointCount() int {
	epValue := p.endpoints.Load()
	endpoints, ok := epValue.([]string)
	if !ok {
		return 0
	}

	return len(endpoints)
}

func (p *connPool) getAvailableEndpointCount() int {
	unavailableEndpointNum := 0
	p.unavailable.Range(func(key, value any) bool {
		unavailableEndpointNum++
		return true
	})

	epValue := p.endpoints.Load()
	endpoints, ok := epValue.([]string)
	if !ok {
		return 0
	}

	return len(endpoints) - unavailableEndpointNum
}

func (p *connPool) getExpectedConnPerEndpoint() int {
	// current conn count, 'cause our conn is delayed closed, curConnCount may include the ones are being closing, and basically bigger than p.requiredConnNum
	curConnCount := p.getConnCount()
	p.log.Debug("curConnCount: ", curConnCount)
	if curConnCount <= 0 {
		return 1
	}

	// initial conn count
	initConnCount := float64(p.requiredConnNum)
	p.log.Debug("initConnCount: ", initConnCount)

	// average conn count, as curConnCount may be not accurate, we use avgConnCount as a reference
	avgConnCount := (curConnCount + p.requiredConnNum) >> 1
	p.log.Debug("avgConnCount: ", avgConnCount)
	if avgConnCount <= 0 {
		return 1
	}

	// available endpoint count
	availableEndpointCount := p.getAvailableEndpointCount()
	p.log.Debug("availableEndpointCount: ", availableEndpointCount)
	if availableEndpointCount <= 0 {
		return 1
	}

	// curConnCount/availableEndpointCount, estimate a new conn count per endpoint
	estimatedVal := math.Floor(float64(curConnCount) / float64(availableEndpointCount))
	p.log.Debug("conns per endpoint by current conn count: ", estimatedVal)

	// avgConnCount/availableEndpointCount, as a reference value
	averageVal := math.Floor(float64(avgConnCount) / float64(availableEndpointCount))
	p.log.Debug("conns per endpoint by average conn count: ", averageVal)

	// initial conn count per endpoint
	initialVal := float64(p.connsPerEndpoint)
	p.log.Debug("conns per endpoint of initialization: ", initialVal)

	result := averageVal // nolint:ineffassign
	if estimatedVal < initialVal {
		// if estimatedVal is less than initialVal, it indicates new endpoints are added,
		// we need to add new conn for the newly added endpoints, delete conn for old endpoints,
		// but we do not add/delete too more each time, just the min of the estimatedVal and averageVal
		result = math.Min(estimatedVal, averageVal)
	} else {
		// if estimatedVal is less than initialVal, it indicates new endpoints are deleted,
		// we need to delete conn for the deleted endpoints, add conn for the left endpoints,
		// but we do not add/delete too more each time, just the min of the initialVal and averageVal
		result = math.Max(initialVal, averageVal)
	}

	// at least 1 conn
	result = math.Max(1, result)
	p.log.Debug("expectedConnPerEndpoint: ", result)
	return int(result)
}

func (p *connPool) rebalance() {
	expectedConnPerEndpoint := p.getExpectedConnPerEndpoint()
	if expectedConnPerEndpoint <= 0 {
		return
	}

	rebalanced := false
	p.endpointConnCounts.Range(func(key, value any) bool {
		addr := key.(string)
		currentCount := value.(int)
		if currentCount < expectedConnPerEndpoint {
			// if the endpoint is deleted, skip it
			if _, ok := p.endpointMap.Load(addr); !ok {
				return true
			}

			// add new conn
			for i := currentCount; i < expectedConnPerEndpoint; i++ {
				err := p.appendNewConn(addr)
				if err != nil {
					continue
				}
				rebalanced = true
			}
		} else if currentCount > expectedConnPerEndpoint {
			rebalanced = true
			// reduce conn
			p.removeEndpointConn(addr, currentCount-expectedConnPerEndpoint)
		}
		return true
	})

	p.endpointMap.Range(func(key, value any) bool {
		addr := key.(string)
		if _, ok := p.endpointConnCounts.Load(key); ok {
			return true
		}
		for i := 0; i < expectedConnPerEndpoint; i++ {
			err := p.appendNewConn(addr)
			if err != nil {
				continue
			}
			rebalanced = true
		}
		return true
	})

	if rebalanced {
		p.log.Debug("rebalance triggered")
	}
}

func (p *connPool) appendNewConn(addr string) error {
	if addr == "" {
		return errors.New("addr is empty")
	}

	conn, err := p.dialNewConn(addr)
	if err != nil {
		p.log.Warn("failed to add connection, addr: ", addr, ", err: ", err)
		return err
	}

	p.log.Debug("adding connection for addr: ", addr)
	p.put(conn, nil, true)
	return nil
}

func (p *connPool) removeEndpointConn(addr string, count int) {
	var leftConns []gnet.Conn
	var removed int
loop:
	for i := 0; i < cap(p.connChan); i++ {
		select {
		case conn := <-p.connChan:
			remoteAddr := conn.RemoteAddr()
			if remoteAddr == nil {
				continue
			}

			if remoteAddr.String() == addr {
				p.log.Debug("reducing connection for addr: ", addr)
				// we do not decrease conn count here, if the frequence of rebalancing is less then defaultConnCloseDelay, may lead to an inaccurate expected conn count per endpoint
				CloseConn(conn, defaultConnCloseDelay)
				removed++
				if removed >= count {
					break loop
				}

				continue
			}

			// not the conn to remove, put it back
			leftConns = append(leftConns, conn)
		default:
			// no more conn, exit the loop
			break loop
		}
	}

	for _, conn := range leftConns {
		select {
		case p.connChan <- conn:
		default:
			CloseConn(conn, defaultConnCloseDelay)
		}
	}
}

// Close closes the conn pool
func (p *connPool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeCh)

		// close all the conns
		for {
			select {
			case conn := <-p.connChan:
				CloseConn(conn, 0)
			default:
				return
			}
		}
	})
}
