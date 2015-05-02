package redis

import (
	redigo "github.com/garyburd/redigo/redis"
)

var ErrNil = redigo.ErrNil

// Commands with results
type Commands interface {
	KeyCommands
	StringCommands
	HashCommands
	ListCommands
	SetCommands
	SortedSetCommands
	HyperLogLogCommands
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
}

type KeyBatchCommands interface {
	Del(keys ...string) error
	Exists(key string) error
	Expire(key string, seconds int) error
	Rename(key, newKey string) error
	RenameNX(key, newKey string) error
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
	HIncrBy(key string, field string, value int64) (newValue int64, err error)
	HSet(key string, field string, value string) (isNew bool, err error)
	HDel(key string, field string) (bool, error)
}

type HashBatchCommands interface {
	HGet(key string, field string) error
	HIncrBy(key string, field string, value int64) error
	HSet(key string, field string, value string) error
	HDel(key string, field string) error
}

// Lists - http://redis.io/commands#list
type ListCommands interface {
	BLPop(timeout int, keys ...string) (listName string, value string, err error)
	BRPop(timeout int, keys ...string) (listName string, value string, err error)
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
	ZAdd(key string, score float64, value string) (int, error)
	ZCard(key string) (int, error)
	ZRangeByScore(key, start, stop string, options ...interface{}) ([]string, error)
	ZRangeByScoreWithLimit(key, start, stop string, offset, count int) ([]string, error)
	ZRem(key string, members ...string) (removed int, err error)
	ZIncBy(key string, score float64, value string) (int, error)
}

type SortedSetBatchCommands interface {
	ZAdd(key string, score float64, value string) error
	ZIncBy(key string, score float64, value string) error
	ZRem(key string, members ...string) error
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

type PubSub interface {
	// TBD
}
