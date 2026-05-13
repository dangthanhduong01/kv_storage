package thriftpool

import (
	"errors"
	"fmt"
	"sync"
)

type MapPool struct {
	Dial  ThriftDial
	Close ThriftClose

	lock *sync.Mutex

	idleTimeout uint32
	connTimeout uint32
	maxConn     uint32

	pools map[string]*ThriftPool
}

func NewMapPool(maxConn, connTimeout, idleTimeout uint32,
	dial ThriftDial, closeFunc ThriftClose) *MapPool {

	return &MapPool{
		Dial:        dial,
		Close:       closeFunc,
		maxConn:     maxConn,
		idleTimeout: idleTimeout,
		connTimeout: connTimeout,
		pools:       make(map[string]*ThriftPool),
		lock:        new(sync.Mutex),
	}
}

func (mp *MapPool) getServerPool(ip, port string) (*ThriftPool, error) {
	addr := fmt.Sprintf("%s:%s", ip, port)
	mp.lock.Lock()
	serverPool, ok := mp.pools[addr]
	if !ok {
		mp.lock.Unlock()
		err := errors.New(fmt.Sprintf("Addr:%s thrift pool not exist", addr))
		return nil, err
	}
	mp.lock.Unlock()
	return serverPool, nil
}

func (mp *MapPool) Get(ip, port string) *ThriftPool {
	serverPool, err := mp.getServerPool(ip, port)
	if err != nil {
		addr := fmt.Sprintf("%s:%s", ip, port)
		serverPool = NewThriftPool(ip,
			port,
			mp.maxConn,
			mp.connTimeout,
			mp.idleTimeout,
			mp.Dial,
			mp.Close,
		)
		mp.lock.Lock()
		mp.pools[addr] = serverPool
		mp.lock.Unlock()
	}
	return serverPool
}

func (mp *MapPool) Release(ip, port string) error {
	serverPool, err := mp.getServerPool(ip, port)
	if err != nil {
		return err
	}

	mp.lock.Lock()
	delete(mp.pools, fmt.Sprintf("%s:%s", ip, port))
	mp.lock.Unlock()

	serverPool.Release()

	return nil
}

func (mp *MapPool) ReleaseAll() {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	for _, serverPool := range mp.pools {
		serverPool.Release()
	}
}
