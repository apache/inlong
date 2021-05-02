package transport

import (
	"context"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/codec"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/multiplexing"
)

// ClientTransportOptions represents the transport options
type ClientTransportOptions struct {
	Address string

	CACertFile    string
	TLSCertFile   string
	TLSKeyFile    string
	TLSServerName string
}

// ClientTransport is the transport layer to TubeMQ which is used to communicate with TubeMQ
type ClientTransport struct {
	opts *ClientTransportOptions
	Pool *multiplexing.Pool
}

// SendAndReceive sends the request and receive the response
func (c *ClientTransport) SendAndReceive(ctx context.Context, serial uint32, req []byte) (codec.TransportResponse, error) {
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
