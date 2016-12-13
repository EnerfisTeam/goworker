package goworker

import (
	"errors"
	"net/url"
	"time"

	sent "github.com/FZambia/go-sentinel"
	"github.com/garyburd/redigo/redis"
	"github.com/youtube/vitess/go/pools"
	"strings"
)

var (
	errorInvalidScheme     = errors.New("invalid Redis+Sentinel database URI scheme")
	// https://pypi.python.org/pypi/Redis-Sentinel-Url/1.0.0
	// https://github.com/mp911de/lettuce/wiki/Redis-URI-and-connection-details
	errorMasterNameMissing = errors.New("master set name missing, use redis+sentinel://pass@host1:port1,host2:port2/master_set_name/db")
)

type RedisConn struct {
	redis.Conn
}

type Sentinel struct {
	sent.Sentinel
	Db       string
}

func (r *RedisConn) Close() {
	_ = r.Conn.Close()
}

func newRedisPool(uriString string, capacity int, maxCapacity int, idleTimeout time.Duration) (*pools.ResourcePool, error) {
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

	sentinel := &Sentinel{
		Sentinel: sent.Sentinel{
			Addrs:      hosts,
			MasterName: masterName,
			Dial: func(addr string) (redis.Conn, error) {
				timeout := idleTimeout / 2
				c, err := redis.DialTimeout("tcp", addr, timeout, timeout, timeout)
				return c, err
			},
		},
		Db:       db,
	}

	return pools.NewResourcePool(
		newRedisFactory(sentinel, idleTimeout),
		capacity,
		maxCapacity,
		idleTimeout,
	), nil
}

func newRedisFactory(sentinel *Sentinel, idleTimeout time.Duration) pools.Factory {
	return func() (pools.Resource, error) {
		return redisConnFromURI(sentinel, idleTimeout)
	}
}

func redisConnFromURI(sentinel *Sentinel, idleTimeout time.Duration) (*RedisConn, error) {
	masterAddr, err := sentinel.MasterAddr()
	if err != nil {
		return nil, err
	}

	conn, err := redis.DialTimeout("tcp", masterAddr, idleTimeout, idleTimeout, idleTimeout)
	if err != nil {
		return nil, err
	}

	if sentinel.Db != "" {
		_, err := conn.Do("SELECT", sentinel.Db)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	c := &RedisConn{Conn: conn}
	return c, nil
}
