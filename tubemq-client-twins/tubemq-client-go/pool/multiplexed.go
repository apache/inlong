/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net"
	"sync"
	"time"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/codec"
)

var DefaultMultiplexedPool = New()

var (
	// ErrConnClosed indicates that the connection is closed
	ErrConnClosed = errors.New("connection is closed")
	// ErrChanClose indicates the recv chan is closed
	ErrChanClose = errors.New("unexpected recv chan close")
	// ErrWriteBufferDone indicates write buffer done
	ErrWriteBufferDone = errors.New("write buffer done")
	// ErrAssertConnectionFail indicates connection assertion error
	ErrAssertConnectionFail = errors.New("assert connection slice fail")
)

const (
	Initial int = iota
	Connected
	Closing
	Closed
)

var queueSize = 10000

func New() *Multiplexed {
	m := &Multiplexed{
		connections: new(sync.Map),
	}
	return m
}

type writerBuffer struct {
	buffer chan []byte
	done   <-chan struct{}
}

func (w *writerBuffer) get() ([]byte, error) {
	select {
	case req := <-w.buffer:
		return req, nil
	case <-w.done:
		return nil, ErrWriteBufferDone
	}
}

type recvReader struct {
	ctx  context.Context
	recv chan *codec.FrameResponse
}

type MultiplexedConnection struct {
	serialNo uint32
	conn     *Connection
	reader   *recvReader
	done     chan struct{}
}

func (mc *MultiplexedConnection) Write(b []byte) error {
	if err := mc.conn.send(b); err != nil {
		mc.conn.remove(mc.serialNo)
		return err
	}
	return nil
}

func (mc *MultiplexedConnection) Read() (*codec.FrameResponse, error) {
	select {
	case <-mc.reader.ctx.Done():
		mc.conn.remove(mc.serialNo)
		return nil, mc.reader.ctx.Err()
	case v, ok := <-mc.reader.recv:
		if ok {
			return v, nil
		}
		if mc.conn.err != nil {
			return nil, mc.conn.err
		}
		return nil, ErrChanClose
	case <-mc.done:
		return nil, mc.conn.err
	}
}

func (mc *MultiplexedConnection) recv(rsp *codec.FrameResponse) {
	mc.reader.recv <- rsp
	mc.conn.remove(rsp.GetSerialNo())
}

type DialOptions struct {
	Network       string
	Address       string
	Timeout       time.Duration
	CACertFile    string
	TLSCertFile   string
	TLSKeyFile    string
	TLSServerName string
}

type Connection struct {
	err         error
	address     string
	mu          sync.RWMutex
	connections map[uint32]*MultiplexedConnection
	framer      *codec.Framer
	conn        net.Conn
	done        chan struct{}
	mDone       chan struct{}
	buffer      *writerBuffer
	dialOpts    *DialOptions
	state       int
	multiplexed *Multiplexed
}

func (c *Connection) new(ctx context.Context, serialNo uint32) (*MultiplexedConnection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}

	vc := &MultiplexedConnection{
		serialNo: serialNo,
		conn:     c,
		done:     c.mDone,
		reader: &recvReader{
			ctx:  ctx,
			recv: make(chan *codec.FrameResponse, 1),
		},
	}

	if prevConn, ok := c.connections[serialNo]; ok {
		close(prevConn.reader.recv)
	}
	c.connections[serialNo] = vc
	return vc, nil
}

func (c *Connection) close(lastErr error, done chan struct{}) {
	if lastErr == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == Closed {
		return
	}

	select {
	case <-done:
		return
	default:
	}

	c.state = Closing
	c.err = lastErr
	c.connections = make(map[uint32]*MultiplexedConnection)
	close(c.done)
	if c.conn != nil {
		c.conn.Close()
	}
	err := c.reconnect()
	if err != nil {
		c.state = Closed
		close(c.mDone)
		c.multiplexed.connections.Delete(c)
	}
}

func (c *Connection) reconnect() error {
	conn, err := dialWithTimeout(c.dialOpts)
	if err != nil {
		return err
	}
	c.done = make(chan struct{})
	c.conn = conn
	c.framer = codec.New(conn)
	c.buffer.done = c.done
	c.state = Connected
	c.err = nil
	go c.reader()
	go c.writer()
	return nil
}

func (c *Connection) writer() {
	var lastErr error
	for {
		select {
		case <-c.done:
			return
		default:
		}
		req, err := c.buffer.get()
		if err != nil {
			lastErr = err
			break
		}
		if err := c.write(req); err != nil {
			lastErr = err
			break
		}
	}
	c.close(lastErr, c.done)
}

func (c *Connection) send(b []byte) error {
	if c.state == Closed {
		return ErrConnClosed
	}

	select {
	case c.buffer.buffer <- b:
		return nil
	case <-c.mDone:
		return c.err
	}
}

func (c *Connection) remove(id uint32) {
	c.mu.Lock()
	delete(c.connections, id)
	c.mu.Unlock()
}

func (c *Connection) write(b []byte) error {
	sent := 0
	for sent < len(b) {
		n, err := c.conn.Write(b[sent:])
		if err != nil {
			return err
		}
		sent += n
	}
	return nil
}

func (c *Connection) reader() {
	var lastErr error
	for {
		select {
		case <-c.done:
			return
		default:
		}
		rsp, err := c.framer.Decode()
		if err != nil {
			lastErr = err
			break
		}
		serialNo := rsp.GetSerialNo()
		c.mu.RLock()
		mc, ok := c.connections[serialNo]
		c.mu.RUnlock()
		if !ok {
			continue
		}
		mc.reader.recv <- rsp
		mc.conn.remove(rsp.GetSerialNo())
	}
	c.close(lastErr, c.done)
}

type Multiplexed struct {
	connections *sync.Map
}

func (p *Multiplexed) Get(ctx context.Context, address string, serialNo uint32) (*MultiplexedConnection, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if v, ok := p.connections.Load(address); ok {
		if c, ok := v.(*Connection); ok {
			return c.new(ctx, serialNo)
		}
		return nil, ErrAssertConnectionFail
	}

	c := &Connection{
		address:     address,
		connections: make(map[uint32]*MultiplexedConnection),
		done:        make(chan struct{}),
		mDone:       make(chan struct{}),
		state:       Initial,
	}
	c.buffer = &writerBuffer{
		buffer: make(chan []byte, queueSize),
		done:   c.done,
	}
	p.connections.Store(address, c)

	conn, dialOpts, err := dial(ctx, address)
	c.dialOpts = dialOpts
	if err != nil {
		return nil, err
	}
	c.framer = codec.New(conn)
	c.conn = conn
	c.state = Connected
	go c.reader()
	go c.writer()
	return c.new(ctx, serialNo)
}

func dial(ctx context.Context, address string) (net.Conn, *DialOptions, error) {
	var timeout time.Duration
	t, ok := ctx.Deadline()
	if ok {
		timeout = t.Sub(time.Now())
	}
	dialOpts := &DialOptions{
		Network: "tcp",
		Address: address,
		Timeout: timeout,
	}
	select {
	case <-ctx.Done():
		return nil, dialOpts, ctx.Err()
	default:
	}
	conn, err := dialWithTimeout(dialOpts)
	return conn, dialOpts, err
}

func dialWithTimeout(opts *DialOptions) (net.Conn, error) {
	if len(opts.CACertFile) == 0 {
		return net.DialTimeout(opts.Network, opts.Address, opts.Timeout)
	}

	tlsConf := &tls.Config{}
	if opts.CACertFile == "none" {
		tlsConf.InsecureSkipVerify = true
	} else {
		if len(opts.TLSServerName) == 0 {
			opts.TLSServerName = opts.Address
		}
		tlsConf.ServerName = opts.TLSServerName
		certPool, err := getCertPool(opts.CACertFile)
		if err != nil {
			return nil, err
		}

		tlsConf.RootCAs = certPool

		if len(opts.TLSCertFile) != 0 {
			cert, err := tls.LoadX509KeyPair(opts.TLSCertFile, opts.TLSKeyFile)
			if err != nil {
				return nil, err
			}
			tlsConf.Certificates = []tls.Certificate{cert}
		}
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: opts.Timeout}, opts.Network, opts.Address, tlsConf)
}

func getCertPool(caCertFile string) (*x509.CertPool, error) {
	if caCertFile != "root" {
		ca, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM(ca)
		if !ok {
			return nil, err
		}
		return certPool, nil
	}
	return nil, nil
}
