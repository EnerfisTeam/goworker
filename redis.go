package goworker

import (
	"errors"
	"net/url"
	"time"

	sent "github.com/FZambia/go-sentinel"
	"github.com/garyburd/redigo/redis"
	"github.com/youtube/vitess/go/pools"
	"strings"
	"fmt"
	"golang.org/x/net/context"
)

var (
	errorInvalidScheme = errors.New("invalid Redis+Sentinel database URI scheme")
	// https://pypi.python.org/pypi/Redis-Sentinel-Url/1.0.0
	// https://github.com/mp911de/lettuce/wiki/Redis-URI-and-connection-details
	errorMasterNameMissing = errors.New("master set name missing, use redis+sentinel://pass@host1:port1,host2:port2/master_set_name/db")
)

type RedisConn struct {
	redis.Conn
}

func (r *RedisConn) Close() {
	_ = r.Conn.Close()
}

type Sentinel struct {
	sentinel *sent.Sentinel
	db          string
	pool        *pools.ResourcePool
	capacity    int
	maxCapacity int
	idleTimeout time.Duration
}

func NewSentinel(uriString string, capacity, maxCapacity int, idleTimeout time.Duration) (*Sentinel, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, err
	}

	var hosts []string
	var masterName, db string

	switch uri.Scheme {
	case "redis+sentinel":
		hosts = strings.Split(uri.Host, ",")
		parts := strings.Split(uri.Path, "/")
		if len(parts) < 2 {
			return nil, errorMasterNameMissing
		}
		masterName = parts[1]
		if len(parts) >= 3 {
			db = parts[2]
		}
	default:
		return nil, errorInvalidScheme
	}

	sentinel := &sent.Sentinel{
		Addrs:      hosts,
		MasterName: masterName,
		Dial: func(addr string) (redis.Conn, error) {
			timeout := idleTimeout / 2
			return redis.DialTimeout("tcp", addr, timeout, timeout, timeout)
		},
	}

	return &Sentinel{
		sentinel: sentinel,
		db: db,
		capacity: capacity,
		maxCapacity: maxCapacity,
		idleTimeout: idleTimeout,
	}, nil
}

func (s *Sentinel) getPool() *pools.ResourcePool {
	if s.pool == nil {
		s.pool = pools.NewResourcePool(
			s.newRedisFactory(),
			s.capacity,
			s.maxCapacity,
			s.idleTimeout,
		)
	}
	return s.pool
}

func (s *Sentinel) GetConn(ctx context.Context) (*RedisConn, error) {
	resource, err := s.getPool().Get(ctx)
	if err != nil {
		return nil, err
	}
	conn := resource.(*RedisConn)
	if conn == nil {
		return nil, errors.New("No connection available")
	}
	return conn, nil
}

func (s *Sentinel) PutConn(conn *RedisConn) {
	if s.pool == nil {
		return
	}
	s.pool.Put(conn)
}

func (s *Sentinel) Discover() error {
	return s.sentinel.Discover()
}

func (s *Sentinel) Close() {
	s.sentinel.Close()
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Sentinel) newRedisFactory() pools.Factory {
	return func() (pools.Resource, error) {
		return s.redisConnFromURI()
	}
}

func (s *Sentinel) redisConnFromURI() (*RedisConn, error) {
	masterAddr, err := s.sentinel.MasterAddr()
	if err != nil {
		return nil, err
	}

	conn, err := redis.DialTimeout(
		"tcp",
		masterAddr,
		s.idleTimeout,
		s.idleTimeout,
		s.idleTimeout,
	)
	if err != nil {
		return nil, err
	}

	if s.db != "" {
		_, err := conn.Do("SELECT", s.db)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	c := &RedisConn{Conn: conn}
	return c, nil
}

func role(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch reply := reply.(type) {
	case []interface{}:
		if len(reply) == 0 {
			return "", fmt.Errorf("redigo: unexpected element type for role (string), got type %T", reply)
		}
		result, ok := reply[0].([]byte)
		if !ok {
			return "", fmt.Errorf("redigo: unexpected element type for role (string), got type %T", reply[0])
		}
		return string(result), nil
	case nil:
		return "", redis.ErrNil
	case redis.Error:
		return "", reply
	}
	return "", fmt.Errorf("redigo: unexpected type for role (string), got type %T", reply)
}
