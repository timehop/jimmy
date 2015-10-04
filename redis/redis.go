package redis

import (
	redigo "github.com/garyburd/redigo/redis"
)

var (
	ErrNil = redigo.ErrNil

	redigoErrNoAuth   = redigo.Error("NOAUTH Authentication required.")
	redigoErrSentAuth = redigo.Error("ERR Client sent AUTH, but no password is set")
)

// Commands with results
type Commands interface {
	KeyCommands
	StringCommands
	HashCommands
	ListCommands
	SetCommands
	SortedSetCommands
	HyperLogLogCommands
	ScanCommands
}

// Commands with no results, to be used in transactions/pipelining.
type BatchCommands interface {
	KeyBatchCommands
	StringBatchCommands
	HashBatchCommands
	ListBatchCommands
	SetBatchCommands
	SortedSetBatchCommands
	HyperLogLogBatchCommands
}

type Transactions interface {
	Multi() error
	Exec() ([]interface{}, error)
	// Discard() error
	// Watch(keys ...string) error
	// Unwatch() error
}

// Keys - http://redis.io/commands#generic
type KeyCommands interface {
	Del(keys ...string) (int, error)
	Exists(key string) (bool, error)
	Expire(key string, seconds int) (bool, error)
	Rename(key, newKey string) error
	RenameNX(key, newKey string) (bool, error)
	TTL(key string) (int, error)
}

type KeyBatchCommands interface {
	Del(keys ...string) error
	Exists(key string) error
	Expire(key string, seconds int) error
	Rename(key, newKey string) error
	RenameNX(key, newKey string) error
	TTL(key string) error
}

// Strings - http://redis.io/commands#string
type StringCommands interface {
	Get(key string) (string, error)
	Set(key, value string) error
	SetEx(key, value string, expire int) error
	Incr(key string) (int, error)
}

type StringBatchCommands interface {
	Get(key string) error
	Set(key, value string) error
	SetEx(key, value string, expire int) error
	Incr(key string) error
}

// Hashes - http://redis.io/commands#hash
type HashCommands interface {
	HGet(key string, field string) (string, error)

	// HGetAll returns a map containing all the fields and values in the specified key.
	// As specified by the Redis docs, because non-existing keys are treated as empty hashes,
	// calling this on a non-existant key will return an empty map and a nil error.
	HGetAll(key string) (map[string]string, error)

	HIncrBy(key string, field string, value int64) (newValue int64, err error)
	HSet(key string, field string, value string) (isNew bool, err error)

	// HMGet returns a map of the specified fields to their values in the specified key. As is
	// consistent with redigo.Strings, nil values (missing fields) are converted to empty strings.
	// As specified by the Redis docs, because non-existing keys are treated as empty hashes,
	// calling this on a non-existant key will return a map of the specified keys to empty strings.
	HMGet(key string, fields ...string) (map[string]string, error)

	HMSet(key string, args map[string]interface{}) error
	HDel(key string, field string) (bool, error)
}

type HashBatchCommands interface {
	HGet(key string, field string) error
	HGetAll(key string) error
	HIncrBy(key string, field string, value int64) error
	HSet(key string, field string, value string) error
	HMGet(key string, fields ...string) error
	HMSet(key string, args map[string]interface{}) error
	HDel(key string, field string) error
}

// Lists - http://redis.io/commands#list
type ListCommands interface {
	BLPop(timeout int, keys ...string) (listName string, value string, err error)
	BRPop(timeout int, keys ...string) (listName string, value string, err error)
	LIndex(key string, index int) (string, error)
	LLen(key string) (int, error)
	LPop(key string) (string, error)
	LPush(key string, values ...string) (int, error)
	LTrim(key string, startIndex int, endIndex int) error
	LRange(key string, startIndex int, endIndex int) ([]string, error)
	RPop(key string) (string, error)
	RPush(key string, values ...string) (int, error)
}

type ListBatchCommands interface {
	LPop(key string) error
	LPush(key string, values ...string) error
	LTrim(key string, startIndex int, endIndex int) error
	LRange(key string, startIndex int, endIndex int) error
	RPop(key string) error
	RPush(key string, values ...string) error
}

// Sets - http://redis.io/commands#set
type SetCommands interface {
	SAdd(key string, member string, members ...string) (int, error)
	SCard(key string) (int, error)
	SRem(key, member string, members ...string) (int, error)
	SPop(key string) (string, error)
	SMembers(key string) ([]string, error)
	SRandMember(key string, count int) ([]string, error)
	SDiff(key string, keys ...string) ([]string, error)
	SIsMember(key string, member string) (bool, error)
	SMove(source, destination, member string) (bool, error)
}

type SetBatchCommands interface {
	SAdd(key string, member string, members ...string) error
	SRem(key string, member string, members ...string) error
	SPop(key string) error
	SMembers(key string) error
	SRandMember(key string, count int) error
	SDiff(key string, keys ...string) error
	SMove(source, destination, member string) error
}

// Sorted Sets - http://redis.io/commands#sorted_set
type SortedSetCommands interface {
	ZAdd(key string, args ...interface{}) (int, error)
	ZCard(key string) (int, error)
	ZRangeByScore(key, start, stop string, options ...interface{}) ([]string, error)
	ZRevRangeByScore(key, start, stop string, options ...interface{}) ([]string, error)
	ZRangeByScoreWithLimit(key, start, stop string, offset, count int) ([]string, error)
	ZRank(key, member string) (int, error)
	ZRem(key string, members ...string) (removed int, err error)
	ZRemRangeByRank(key string, start, stop int) (int, error)
	ZScore(key string, member string) (score float64, err error)
	ZIncrBy(key string, score float64, value string) (int, error)
}

type SortedSetBatchCommands interface {
	ZAdd(key string, args ...interface{}) error
	ZIncrBy(key string, score float64, value string) error
	ZRank(key, member string) error
	ZRem(key string, members ...string) error
	ZRemRangeByRank(key string, start, stop int) error
}

// HyperLogLog
type HyperLogLogCommands interface {
	PFAdd(key string, values ...string) (int, error)
	PFCount(key string) (int, error)
	PFMerge(mergedKey string, keysToMerge ...string) (bool, error)
}

type HyperLogLogBatchCommands interface {
	PFAdd(key string, values ...string) error
	PFCount(key string) error
	PFMerge(mergedKey string, keysToMerge ...string) error
}

type ScanCommands interface {
	SScan(key string, cursor int, match string, count int) (nextCursor int, matches []string, err error)
	ZScan(key string, cursor int, match string, count int) (nextCursor int, matches []string, scores []float64, err error)
}

type PubSub interface {
	// TBD
}
