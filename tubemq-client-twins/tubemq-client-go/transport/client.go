package transport

import (
	"context"

	"github.com/golang/protobuf/proto"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/codec"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/multiplexing"
)

// ClientOptions represents the transport options
type ClientOptions struct {
	Address string

	CACertFile    string
	TLSCertFile   string
	TLSKeyFile    string
	TLSServerName string
}

// Client is the transport layer to TubeMQ which is used to communicate with TubeMQ
type Client struct {
	opts  *ClientOptions
	pool  *multiplexing.Pool
	codec codec.Codec
}

func New(opts *ClientOptions, pool *multiplexing.Pool) *Client {
	return &Client{
		opts:  opts,
		pool:  pool,
		codec: &codec.TubeMQCodec{},
	}
}

// DoRequest sends the request and decode the response
func (c *Client) DoRequest(ctx context.Context, serialNo uint32, req *codec.RpcRequest, reqBody proto.Message) (*codec.RpcResponse, error) {
	opts := &multiplexing.DialOptions{
		Address: c.opts.Address,
		Network: "tcp",
	}
	if c.opts.CACertFile != "" {
		opts.CACertFile = c.opts.CACertFile
		opts.TLSCertFile = c.opts.TLSCertFile
		opts.TLSKeyFile = c.opts.TLSKeyFile
		opts.TLSServerName = c.opts.TLSServerName
	}

	conn, err := c.pool.Get(ctx, c.opts.Address, serialNo, opts)
	if err != nil {
		return nil, err
	}

	b, err := c.encodeRequest(serialNo, req, reqBody)
	if err != nil {
		return nil, err
	}

	if err := conn.Write(b); err != nil {
		return nil, err
	}

	rsp, err := conn.Read()
	if err != nil {
		return nil, err
	}
	return c.codec.Decode(rsp)
}

func (c *Client) encodeRequest(serialNo uint32, req *codec.RpcRequest, reqBody proto.Message) ([]byte, error) {
	body, err := proto.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req.RequestBody.Request = body
	b, err := c.codec.Encode(serialNo, req)
	if err != nil {
		return nil, err
	}
	return b, nil
}
