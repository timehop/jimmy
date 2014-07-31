package redis

import (
	"errors"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/soveran/redisurl"
)

const Unlimited = 0

var (
	DefaultConfig = Config{
		MaxOpenConnections: Unlimited,
		MaxIdleConnections: 10,
		IdleTimeout:        0,
	}

	ErrPoolExhausted = errors.New("connection pool exhausted")
)

type Config struct {
	MaxOpenConnections int
	MaxIdleConnections int
	IdleTimeout        time.Duration
}

type PooledConnection interface {
	Connection

	Release()
}

type Pool interface {
	Commands

	GetConnection() (PooledConnection, error)
	Return(PooledConnection)

	Do(f func(Connection)) error
	Transaction(func(Transaction)) ([]interface{}, error)
	Pipelined(func(Pipeline)) ([]interface{}, error)
	PipelinedDiscarding(f func(Pipeline)) error

	Shutdown()
}

func NewPool(url string, config Config) Pool {
	generator := func() (redigo.Conn, error) {
		return redisurl.ConnectToURL(url)
	}

	p := redigo.NewPool(generator, config.MaxIdleConnections)
	p.MaxActive = config.MaxOpenConnections
	p.IdleTimeout = config.IdleTimeout

	return &pool{p: p}
}

type pool struct {
	p *redigo.Pool
}

func (s *pool) GetConnection() (PooledConnection, error) {
	c := s.p.Get()

	// Force acquisition of an underlying connection:
	// https://github.com/garyburd/redigo/blob/master/redis/pool.go#L138
	if err := c.Err(); err != nil {
		c.Close()
		if err.Error() == "redigo: connection pool exhausted" {
			return nil, ErrPoolExhausted
		} else {
			return nil, err
		}
	}

	return &connection{pool: s, c: c}, nil
}

func (s *pool) Return(c PooledConnection) {
	if c == nil {
		return
	}

	c.Release()
}

func (s *pool) Do(f func(Connection)) error {
	c, err := s.GetConnection()
	if err != nil {
		return err
	}

	defer s.Return(c)

	f(c)

	return nil
}

func (s *pool) Transaction(f func(Transaction)) ([]interface{}, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}

	defer s.Return(c)

	return c.Transaction(f)
}

func (s *pool) Pipelined(f func(Pipeline)) ([]interface{}, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}

	defer s.Return(c)

	return c.Pipelined(f)
}

func (s *pool) PipelinedDiscarding(f func(Pipeline)) error {
	c, err := s.GetConnection()
	if err != nil {
		return err
	}

	defer s.Return(c)

	return c.PipelinedDiscarding(f)
}

func (s *pool) Shutdown() {
	s.p.Close()
}

// Commands - Keys

func (s *pool) Del(keys ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.Del(keys...)
}

func (s *pool) Exists(key string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.Exists(key)
}

// Commands - Strings

func (s *pool) Get(key string) (string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", err
	}
	defer s.Return(c)

	return c.Get(key)
}

func (s *pool) Set(key, value string) error {
	c, err := s.GetConnection()
	if err != nil {
		return err
	}
	defer s.Return(c)

	return c.Set(key, value)
}

func (s *pool) SetEx(key, value string, expire int) error {
	c, err := s.GetConnection()
	if err != nil {
		return err
	}
	defer s.Return(c)

	return c.SetEx(key, value, expire)
}

// Commands - Hashes

func (s *pool) HGet(key, field string) (string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", err
	}
	defer s.Return(c)

	return c.HGet(key, field)
}

func (s *pool) HSet(key string, field string, value string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.HSet(key, field, value)
}

// Commands - Lists

func (s *pool) BLPop(timeout int, keys ...string) (string, string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", "", err
	}
	defer s.Return(c)

	return c.BLPop(timeout, keys...)
}

func (s *pool) BRPop(timeout int, keys ...string) (string, string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", "", err
	}
	defer s.Return(c)

	return c.BRPop(timeout, keys...)
}

func (s *pool) LLen(key string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.LLen(key)
}

func (s *pool) LPop(key string) (string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", err
	}
	defer s.Return(c)

	return c.LPop(key)
}

func (s *pool) LPush(key string, values ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.LPush(key, values...)
}

func (s *pool) RPush(key string, values ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.RPush(key, values...)
}

// Commands - Sets

func (s *pool) SAdd(key string, members ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.SAdd(key, members...)
}

// Commands - Sorted sets

func (s *pool) ZAdd(key string, score float64, value string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.ZAdd(key, score, value)
}

func (s *pool) ZCard(key string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.ZCard(key)
}

func (s *pool) ZRangeByScore(key, start, stop string) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.ZRangeByScore(key, start, stop)
}

func (s *pool) ZRangeByScoreWithLimit(key, start, stop string, offset, count int) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.ZRangeByScoreWithLimit(key, start, stop, offset, count)
}

func (s *pool) ZRem(key string, members ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.ZRem(key, members...)
}
