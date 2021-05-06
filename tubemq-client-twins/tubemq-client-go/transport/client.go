package transport

import (
	"context"

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
	opts *ClientOptions
	Pool *multiplexing.Pool
}

// SendRequest sends the request and receive the response
func (c *Client) SendRequest(ctx context.Context, serial uint32, req []byte) (codec.Response, error) {
	opts := &multiplexing.DialOptions{
		Address: c.opts.Address,
		Network: "tcp",
	}
	if c.opts.CACertFile != "none" {
		opts.CACertFile = c.opts.CACertFile
		opts.TLSCertFile = c.opts.TLSCertFile
		opts.TLSKeyFile = c.opts.TLSKeyFile
		opts.TLSServerName = c.opts.TLSServerName
	}

	conn, err := c.Pool.Get(ctx, c.opts.Address, serial, opts)
	if err != nil {
		return nil, err
	}

	if err := conn.Write(req); err != nil {
		return nil, err
	}

	rsp, err := conn.Read()
	if err != nil {
		return nil, err
	}
	return rsp, err
}
