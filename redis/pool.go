package redis

import (
	"errors"
	netURL "net/url"
	"sync"
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

	hostsNotUsingAuth = &hosts{hosts: map[string]bool{}}
)

// Thread safe
type hosts struct {
	mu    sync.RWMutex
	hosts map[string]bool
}

func (m *hosts) Add(host string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hosts[host] = true
}

func (m *hosts) Remove(host string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.hosts, host)
}

func (m *hosts) Get(host string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hosts[host]
}

func generateConnection(url *netURL.URL) (redigo.Conn, error) {
	// Then we expec the server to not ask for a password
	if hostsNotUsingAuth.Get(url.Host) {
		url.User = nil
		conn, err := redisurl.ConnectToURL(url.String())
		if err == redigoErrNoAuth {
			hostsNotUsingAuth.Remove(url.Host)
			return generateConnection(url)
		}
		return conn, err
	}

	// Then we expect the server to potentially ask for a password
	conn, err := redisurl.ConnectToURL(url.String())
	if err == redigoErrSentAuth {
		hostsNotUsingAuth.Add(url.Host)
		return generateConnection(url)
	}
	return conn, err
}

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

func NewPool(url string, config Config) (Pool, error) {
	parsedRedisURL, err := netURL.Parse(url)
	if err != nil {
		return nil, err
	}

	return NewPoolWithURL(parsedRedisURL, config), nil
}

func NewPoolWithURL(url *netURL.URL, config Config) Pool {
	var password string
	if url.User != nil {
		password, _ = url.User.Password()
	}

	generator := func() (redigo.Conn, error) {
		return generateConnection(url)
	}
	p := redigo.NewPool(generator, config.MaxIdleConnections)
	p.MaxActive = config.MaxOpenConnections
	p.IdleTimeout = config.IdleTimeout

	return &pool{p: p, password: password}
}

type pool struct {
	p        *redigo.Pool
	password string
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

	return &connection{pool: s, c: c, password: s.password}, nil
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

func (s *pool) Expire(key string, seconds int) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.Expire(key, seconds)
}

func (s *pool) Rename(key, newKey string) error {
	c, err := s.GetConnection()
	if err != nil {
		return err
	}
	defer s.Return(c)

	return c.Rename(key, newKey)
}

func (s *pool) RenameNX(key, newKey string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.RenameNX(key, newKey)
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

func (s *pool) Incr(key string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.Incr(key)
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

func (s *pool) HIncrBy(key, field string, value int64) (int64, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.HIncrBy(key, field, value)
}

func (s *pool) HSet(key string, field string, value string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.HSet(key, field, value)
}

func (s *pool) HDel(key string, field string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.HDel(key, field)
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

func (s *pool) LTrim(key string, startIndex int, endIndex int) error {
	c, err := s.GetConnection()
	if err != nil {
		return err
	}
	defer s.Return(c)

	return c.LTrim(key, startIndex, endIndex)
}

func (s *pool) LRange(key string, startIndex int, endIndex int) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.LRange(key, startIndex, endIndex)
}

func (s *pool) RPop(key string) (string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", err
	}
	defer s.Return(c)

	return c.RPop(key)
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

func (s *pool) SAdd(key string, member string, members ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.SAdd(key, member, members...)
}

func (s *pool) SCard(key string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.SCard(key)
}

func (s *pool) SRem(key string, member string, members ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.SRem(key, member, members...)
}

func (s *pool) SPop(key string) (string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return "", err
	}
	defer s.Return(c)

	return c.SPop(key)
}

func (s *pool) SMembers(key string) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.SMembers(key)
}

func (s *pool) SRandMember(key string, count int) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.SRandMember(key, count)
}

func (s *pool) SDiff(key string, keys ...string) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.SDiff(key, keys...)
}

func (s *pool) SIsMember(key string, member string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.SIsMember(key, member)
}

func (s *pool) SMove(source, destination, member string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.SMove(source, destination, member)
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

func (s *pool) ZRangeByScore(key, start, stop string, options ...interface{}) ([]string, error) {
	c, err := s.GetConnection()
	if err != nil {
		return nil, err
	}
	defer s.Return(c)

	return c.ZRangeByScore(key, start, stop, options...)
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

func (s *pool) ZScore(key string, member string) (score float64, err error) {
	if member == "" {
		return 0, nil
	}

	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.ZScore(key, member)
}

func (s *pool) ZIncBy(key string, score float64, value string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.ZIncBy(key, score, value)
}

func (s *pool) PFAdd(key string, values ...string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.PFAdd(key, values...)
}

func (s *pool) PFCount(key string) (int, error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, err
	}
	defer s.Return(c)

	return c.PFCount(key)
}

func (s *pool) PFMerge(mergedKey string, keysToMerge ...string) (bool, error) {
	c, err := s.GetConnection()
	if err != nil {
		return false, err
	}
	defer s.Return(c)

	return c.PFMerge(mergedKey, keysToMerge...)
}

func (s *pool) SScan(key string, cursor int, match string, count int) (nextCursor int, matches []string, err error) {
	c, err := s.GetConnection()
	if err != nil {
		return 0, nil, err
	}
	defer s.Return(c)

	return c.SScan(key, cursor, match, count)
}
