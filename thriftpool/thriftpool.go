package thriftpool

import (
	"container/list"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// This package provides a simple implementation of a Thrift client connection pool.
// It allows you to manage a pool of Thrift client connections, which can be reused across multiple requests.
const CHECKINTERVAL = 60

var nowFunc = time.Now

var (
	ErrOverMax          = errors.New("ErrOverMax")
	ErrInvalidConn      = errors.New("ErrInvalidConn")
	ErrPoolClosed       = errors.New("ErrPoolClosed")
	ErrSocketDisconnect = errors.New("ErrSocketDisconnect")
)

type ThriftDial func(ip, port string, connTimeOut time.Duration) (*IdleClient, error)
type ThriftClose func(client *IdleClient) error

type ThriftPool struct {
	Dial  ThriftDial
	Close ThriftClose

	lock        *sync.Mutex
	idle        list.List
	idleTimeout time.Duration
	connTimeOut time.Duration
	maxConn     uint32
	count       uint32
	ip          string
	port        string
	closed      bool
}

type IdleClient struct {
	Socket *thrift.TSocket
	Client interface{}
	Host   string
	Port   string
}

type idleConn struct {
	c        *IdleClient
	lastUsed time.Time
}

func NewThriftPool(ip, port string,
	maxConn, connTimeOut, idleTimeout uint32,
	dial ThriftDial, closeFunc ThriftClose) *ThriftPool {
	pool := &ThriftPool{
		Dial:        dial,
		Close:       closeFunc,
		lock:        &sync.Mutex{},
		idleTimeout: time.Duration(idleTimeout) * time.Second,
		connTimeOut: time.Duration(connTimeOut) * time.Second,
		maxConn:     maxConn,
		count:       0,
		ip:          ip,
		port:        port,
		closed:      false,
	}

	return pool
}

func (p *ThriftPool) Get() (*IdleClient, error) {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return nil, ErrPoolClosed
	}
	if p.idle.Len() == 0 && p.count >= p.maxConn {
		p.lock.Unlock()
		return nil, ErrOverMax
	}

	if p.idle.Len() == 0 {
		dial := p.Dial
		p.count += 1
		p.lock.Unlock()
		client, err := dial(p.ip, p.port, p.connTimeOut)
		if err != nil {
			p.lock.Lock()
			if p.count > 0 {
				p.count -= 1
			}
			p.lock.Unlock()
			return nil, err
		}
		if !client.Check() {
			p.lock.Lock()
			if p.count > 0 {
				p.count -= 1
			}
			p.lock.Unlock()
			return nil, ErrSocketDisconnect
		}
		return client, nil
	} else {
		ele := p.idle.Back()
		idlec := ele.Value.(*idleConn)
		p.idle.Remove(ele)
		p.lock.Unlock()

		if !idlec.c.Check() {
			p.lock.Lock()
			if p.count > 0 {
				p.count -= 1
			}
			p.lock.Unlock()
			return nil, ErrSocketDisconnect
		}
		return idlec.c, nil
	}
}

func (p *ThriftPool) Put(client *IdleClient) error {
	if client == nil {
		return ErrInvalidConn
	}

	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	if p.count > p.maxConn {
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	if !client.Check() {
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	p.idle.PushBack(&idleConn{
		c:        client,
		lastUsed: nowFunc(),
	})
	p.lock.Unlock()
	return nil
}

func (p *ThriftPool) CloseErrConn(client *IdleClient) {
	if client == nil {
		return
	}

	p.lock.Lock()
	if p.count > 0 {
		p.count -= 1
	}
	p.lock.Unlock()

	p.Close(client)
	client = nil
	return
}

func (p *ThriftPool) CheckTimeOut() {
	p.lock.Lock()
	for p.idle.Len() != 0 {
		ele := p.idle.Back()
		if ele == nil {
			break
		}
		v := ele.Value.(*idleConn)
		if v.lastUsed.Add(p.idleTimeout).After(nowFunc()) {
			break
		}

		// timeout, clear
		p.idle.Remove(ele)
		p.lock.Unlock()
		p.Close(v.c)
		p.lock.Lock()
		if p.count > 0 {
			p.count -= 1
		}
	}
	p.lock.Unlock()
	return
}

func (c *IdleClient) SetConnTimeout(connTimeout uint32) {
	c.Socket.SetConnTimeout(time.Duration(connTimeout) * time.Second)
}

func (c *IdleClient) LocalAddr() net.Addr {
	return c.Socket.Conn().LocalAddr()
}

func (c *IdleClient) RemoteAddr() net.Addr {
	return c.Socket.Conn().RemoteAddr()
}

func (c *IdleClient) Check() bool {
	if c.Socket == nil || c.Client == nil {
		return false
	}
	return c.Socket.IsOpen()
}

func (p *ThriftPool) GetIdleCount() uint32 {
	return uint32(p.idle.Len())
}

func (p *ThriftPool) GetConnCount() uint32 {
	return p.count
}

func (p *ThriftPool) ClearConn() {
	for {
		p.CheckTimeOut()
		time.Sleep(CHECKINTERVAL * time.Second)
	}
}

func (p *ThriftPool) Release() {
	p.lock.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.count = 0
	p.lock.Unlock()

	for iter := idle.Front(); iter != nil; iter = iter.Next() {
		p.Close(iter.Value.(*idleConn).c)
	}
}

func (p *ThriftPool) Recover() {
	p.lock.Lock()
	if p.closed == true {
		p.closed = false
	}
	p.lock.Unlock()
}
