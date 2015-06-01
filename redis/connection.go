package redis

import (
	"fmt"
	netURL "net/url"

	redigo "github.com/garyburd/redigo/redis"
)

type Connection interface {
	Commands
	Transactions

	Send(command string, args ...interface{}) error
	Do(command string, args ...interface{}) (interface{}, error)

	Transaction(func(Transaction)) ([]interface{}, error)
	Pipelined(func(Pipeline)) ([]interface{}, error)
	PipelinedDiscarding(f func(Pipeline)) error

	Flush() error
	Receive() (interface{}, error)
}

type UnpooledConnection interface {
	Connection

	Close()
}

func NewConnection(url *netURL.URL) (UnpooledConnection, error) {

	var password string
	if url.User != nil {
		password, _ = url.User.Password()
	}

	c, err := generateConnection(url)
	if err != nil {
		return nil, err
	}

	conn := &connection{
		password: password,
		c:        c,
	}

	return conn, nil

}

type connection struct {
	c        redigo.Conn
	pool     Pool
	password string
}

// PooledConnection

func (s *connection) Release() {
	s.c.Close()
}

// UnpooledConnection

func (s *connection) Close() {
	if s.pool != nil {
		s.Release()
	} else {
		s.c.Close()
	}
}

// Connection

func (s *connection) Send(command string, args ...interface{}) error {
	return s.c.Send(command, args...)
}

func (s *connection) Do(command string, args ...interface{}) (interface{}, error) {
	val, err := s.c.Do(command, args...)
	if err == redigoErrNoAuth && s.password != "" {
		_, err = s.c.Do("AUTH", s.password)
		if err != nil {
			return nil, err
		}
		val, err = s.c.Do(command, args...)
	}
	return val, err
}

func (s *connection) Transaction(f func(Transaction)) ([]interface{}, error) {
	if err := s.Multi(); err != nil {
		return nil, err
	}

	f(asTransaction(s))

	return s.Exec()
}

func (s *connection) Pipelined(f func(Pipeline)) ([]interface{}, error) {
	p := asPipeline(s)

	f(p)

	if err := s.Flush(); err != nil {
		return nil, err
	}

	return p.receiveAll()
}

func (s *connection) PipelinedDiscarding(f func(Pipeline)) error {
	f(asPipeline(s))

	return s.Flush()
}

func (s *connection) Flush() error {
	return s.c.Flush()
}

func (s *connection) Receive() (interface{}, error) {
	return s.c.Receive()
}

// KeyCommands

func (s *connection) Del(keys ...string) (int, error) {
	return redigo.Int(s.Do("DEL", redigo.Args{}.AddFlat(keys)...))
}

func (s *connection) Exists(key string) (bool, error) {
	return redigo.Bool(s.Do("EXISTS", key))
}

func (s *connection) Expire(key string, seconds int) (bool, error) {
	return redigo.Bool(s.Do("EXPIRE", key, seconds))
}

func (s *connection) Rename(key, newKey string) error {
	_, err := s.Do("RENAME", key, newKey)
	return err
}

func (s *connection) RenameNX(key, newKey string) (bool, error) {
	return redigo.Bool(s.Do("RENAMENX", key, newKey))
}

// StringCommands

func (s *connection) Get(key string) (string, error) {
	return redigo.String(s.Do("GET", key))
}

func (s *connection) Set(key, value string) error {
	_, err := s.Do("SET", key, value)
	return err
}

func (s *connection) SetEx(key, value string, expire int) error {
	_, err := s.Do("SETEX", key, expire, value)
	return err
}

func (s *connection) Incr(key string) (int, error) {
	return redigo.Int(s.Do("INCR", key))
}

// HashCommands

func (s *connection) HGet(key, field string) (string, error) {
	return redigo.String(s.Do("HGET", key, field))
}

func (s *connection) HIncrBy(key, field string, value int64) (int64, error) {
	return redigo.Int64(s.Do("HINCRBY", key, field, value))
}

func (s *connection) HSet(key string, field string, value string) (bool, error) {
	return redigo.Bool(s.Do("HSET", key, field, value))
}

func (s *connection) HDel(key string, field string) (bool, error) {
	return redigo.Bool(s.Do("HDEL", key, field))
}

// ListCommands

func (s *connection) BLPop(timeout int, keys ...string) (string, string, error) {
	reply, err := redigo.Values(s.Do("BLPOP", redigo.Args{}.AddFlat(keys).Add(timeout)...))
	if err != nil {
		return "", "", err
	}

	return string(reply[0].([]byte)), string(reply[1].([]byte)), nil
}

func (s *connection) BRPop(timeout int, keys ...string) (string, string, error) {
	reply, err := redigo.Values(s.Do("BRPOP", redigo.Args{}.AddFlat(keys).Add(timeout)...))
	if err != nil {
		return "", "", err
	}

	return string(reply[0].([]byte)), string(reply[1].([]byte)), nil
}

func (s *connection) LIndex(key string, index int) (string, error) {
	return redigo.String(s.Do("LINDEX", key, index))
}

func (s *connection) LLen(key string) (int, error) {
	return redigo.Int(s.Do("LLEN", key))
}

func (s *connection) LPop(key string) (string, error) {
	return redigo.String(s.Do("LPOP", key))
}

func (s *connection) LPush(key string, values ...string) (int, error) {
	return redigo.Int(s.Do("LPUSH", redigo.Args{key}.AddFlat(values)...))
}

func (s *connection) LTrim(key string, startIndex int, endIndex int) error {
	_, err := s.Do("LTRIM", key, startIndex, endIndex)
	return err
}

func (s *connection) LRange(key string, startIndex int, endIndex int) ([]string, error) {
	return redigo.Strings(s.Do("LRANGE", key, startIndex, endIndex))
}

func (s *connection) RPop(key string) (string, error) {
	return redigo.String(s.Do("RPOP", key))
}

func (s *connection) RPush(key string, values ...string) (int, error) {
	return redigo.Int(s.Do("RPUSH", redigo.Args{key}.AddFlat(values)...))
}

// SetCommands

func (s *connection) SAdd(key string, member string, members ...string) (int, error) {
	return redigo.Int(s.Do("SADD", redigo.Args{key}.Add(member).AddFlat(members)...))
}

func (s *connection) SCard(key string) (int, error) {
	return redigo.Int(s.Do("SCARD", key))
}

func (s *connection) SRem(key string, member string, members ...string) (int, error) {
	return redigo.Int(s.Do("SREM", redigo.Args{key}.Add(member).AddFlat(members)...))
}

func (s *connection) SPop(key string) (string, error) {
	return redigo.String(s.Do("SPOP", key))
}

func (s *connection) SMembers(key string) ([]string, error) {
	return redigo.Strings(s.Do("SMEMBERS", key))
}

func (s *connection) SRandMember(key string, count int) ([]string, error) {
	return redigo.Strings(s.Do("SRANDMEMBER", key, count))
}

func (s *connection) SDiff(key string, keys ...string) ([]string, error) {
	return redigo.Strings(s.Do("SDIFF", redigo.Args{key}.AddFlat(keys)...))
}

func (s *connection) SIsMember(key string, member string) (bool, error) {
	return redigo.Bool(s.Do("SISMEMBER", key, member))
}

func (s *connection) SMove(source, destination, member string) (bool, error) {
	return redigo.Bool(s.Do("SMOVE", source, destination, member))
}

// SortedSetCommands

func (s *connection) ZAdd(key string, score float64, value string) (int, error) {
	// Returns number of elements added, 0 if already exist
	return redigo.Int(s.Do("ZADD", key, score, value))
}

func (s *connection) ZCard(key string) (int, error) {
	return redigo.Int(s.Do("ZCARD", key))
}

func (s *connection) ZRangeByScore(key, start, stop string, options ...interface{}) ([]string, error) {
	return redigo.Strings(s.Do("ZRANGEBYSCORE", redigo.Args{key, start, stop}.AddFlat(options)...))
}

// KC: Deprecated. Please use ZRangeByScore(key, start, stop, "LIMIT", 0, 1)
func (s *connection) ZRangeByScoreWithLimit(key, start, stop string, offset, count int) ([]string, error) {
	return redigo.Strings(s.Do("ZRANGEBYSCORE", key, start, stop, "LIMIT", fmt.Sprint(offset), fmt.Sprint(count)))
}

func (s *connection) ZRem(key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	args := redigo.Args{}.Add(key).AddFlat(members)
	return redigo.Int(s.Do("ZREM", args...))
}

func (s *connection) ZScore(key string, member string) (score float64, err error) {
	if member == "" {
		return 0, nil
	}

	return redigo.Float64(s.Do("ZSCORE", key, member))
}

func (s *connection) ZIncBy(key string, score float64, value string) (int, error) {
	// Returns number of score of the value updated
	return redigo.Int(s.Do("ZINCRBY", key, score, value))
}

// HyperLogLogCommands

func (s *connection) PFAdd(key string, values ...string) (int, error) {
	return redigo.Int(s.Do("PFADD", redigo.Args{key}.AddFlat(values)...))
}

func (s *connection) PFCount(key string) (int, error) {
	return redigo.Int(s.Do("PFCOUNT", key))
}

func (s *connection) PFMerge(mergedKey string, keysToMerge ...string) (bool, error) {
	result, err := redigo.String(s.Do("PFMERGE", redigo.Args{mergedKey}.AddFlat(keysToMerge)...))
	if err != nil || err == ErrNil || result != "OK" {
		return false, err
	}
	return true, nil
}

func (s *connection) SScan(key string, cursor int, match string, count int) (nextCursor int, matches []string, err error) {
	var result []interface{}
	if count < 1 {
		if len(match) == 0 {
			result, err = redigo.Values(s.Do("SSCAN", key, cursor))
		} else {
			result, err = redigo.Values(s.Do("SSCAN", key, cursor, "MATCH", match))
		}
	} else {
		if len(match) == 0 {
			result, err = redigo.Values(s.Do("SSCAN", key, cursor, "COUNT", count))
		} else {
			result, err = redigo.Values(s.Do("SSCAN", key, cursor, "MATCH", match, "COUNT", count))
		}
	}
	if err != nil {
		return 0, nil, err
	}
	if len(result) > 0 {
		nextCursor, err = redigo.Int(result[0], nil)
		if err != nil {
			return 0, nil, err
		}
	}
	if len(result) > 1 {
		matches, err = redigo.Strings(result[1], nil)
		if err != nil {
			return 0, nil, err
		}
	}
	return nextCursor, matches, nil
}

// Transactions

func (s *connection) Multi() error {
	return s.Send("MULTI")
}

func (s *connection) Exec() ([]interface{}, error) {
	return redigo.Values(s.Do("EXEC"))
}
