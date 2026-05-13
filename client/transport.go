package main

import (
	"kvstorage/gen-go/kv"
	"kvstorage/thriftpool"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

func dial(addr, port string, connTimeout time.Duration) (*thriftpool.IdleClient, error) {
	socket, err := thrift.NewTSocketTimeout(addr+":"+port, connTimeout, connTimeout)
	if err != nil {
		return nil, err
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()
	tf, err := transportFactory.GetTransport(socket)
	if err != nil {
		return nil, err
	}

	client := kv.NewStorageServiceClientFactory(tf, protocolFactory)
	if err != nil {
		return nil, err
	}

	err = tf.Open()
	if err != nil {
		return nil, err
	}
	return &thriftpool.IdleClient{
		Client: client,
		Socket: socket,
		Host:   addr,
		Port:   port,
	}, nil
}

func close(c *thriftpool.IdleClient) error {
	err := c.Socket.Close()
	return err
}

var bsGenericMapPool = thriftpool.NewMapPool(1000, 5, 3600, dial, close)

func GetKVClient(addr, port string) *thriftpool.IdleClient {
	client, err := bsGenericMapPool.Get(addr, port).Get()
	if err != nil {
		return nil
	}
	return client
}

func BackToPool(c *thriftpool.IdleClient) {
	if c == nil {
		return
	}
	bsGenericMapPool.Get(c.Host, c.Port).Put(c)
}
func ServiceDisconnect(c *thriftpool.IdleClient) {
	if c == nil {
		return
	}
	bsGenericMapPool.Release(c.Host, c.Port)
}
