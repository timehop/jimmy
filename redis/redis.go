package redis

import (
	redigo "github.com/garyburd/redigo/redis"
)

var ErrNil = redigo.ErrNil

type Commands interface {

	// Keys - http://redis.io/commands#generic

	Del(keys ...string) (int, error)
	Exists(key string) (bool, error)
	Expire(key string, seconds int) (bool, error)
	Rename(key, newKey string) error
	RenameNX(key, newKey string) (bool, error)

	// Strings - http://redis.io/commands#string

	Get(key string) (string, error)
	Set(key, value string) error
	SetEx(key, value string, expire int) error
	Incr(key string) (int, error)

	// Hashes - http://redis.io/commands#hash

	HGet(key string, field string) (string, error)
	HIncrBy(key string, field string, value int64) (newValue int64, err error)
	HSet(key string, field string, value string) (isNew bool, err error)
	HDel(key string, field string) (bool, error)

	// Lists - http://redis.io/commands#list

	BLPop(timeout int, keys ...string) (listName string, value string, err error)
	BRPop(timeout int, keys ...string) (listName string, value string, err error)
	LLen(key string) (int, error)
	LPop(key string) (string, error)
	LPush(key string, values ...string) (int, error)
	RPop(key string) (string, error)
	RPush(key string, values ...string) (int, error)

	// Sets - http://redis.io/commands#set

	SAdd(key string, member string, members ...string) (int, error)
	SCard(key string) (int, error)
	SRem(key, member string, members ...string) (int, error)
	SPop(key string) (string, error)
	SMembers(key string) ([]string, error)
	SRandMember(key string, count int) ([]string, error)
	SDiff(key string, keys ...string) ([]string, error)
	SIsMember(key string, member string) (bool, error)

	// Sorted Sets - http://redis.io/commands#sorted_set

	ZAdd(key string, score float64, value string) (int, error)
	ZCard(key string) (int, error)
	ZRangeByScore(key, start, stop string, options ...interface{}) ([]string, error)
	ZRangeByScoreWithLimit(key, start, stop string, offset, count int) ([]string, error)
	ZRem(key string, members ...string) (removed int, err error)
	ZIncBy(key string, score float64, value string) (int, error)

	// HyperLogLog

	PFAdd(key string, values ...string) (int, error)
	PFCount(key string) (int, error)
	PFMerge(mergedKey string, keysToMerge ...string) (bool, error)
}

type NoResultCommands interface {

	// Keys - http://redis.io/commands#generic

	Del(keys ...string) error
	Exists(key string) error
	Expire(key string, seconds int) error
	Rename(key, newKey string) error
	RenameNX(key, newKey string) error

	// Strings - http://redis.io/commands#string

	Get(key string) error
	Set(key, value string) error
	SetEx(key, value string, expire int) error
	Incr(key string) error

	// Hashes - http://redis.io/commands#hash

	HGet(key string, field string) error
	HIncrBy(key string, field string, value int64) error
	HSet(key string, field string, value string) error
	HDel(key string, field string) error

	// Lists - http://redis.io/commands#list

	LPop(key string) error
	LPush(key string, values ...string) error
	RPop(key string) error
	RPush(key string, values ...string) error

	// Sets - http://redis.io/commands#generic

	SAdd(key string, member string, members ...string) error
	SRem(key string, member string, members ...string) error
	SPop(key string) error
	SMembers(key string) error
	SRandMember(key string, count int) error
	SDiff(key string, keys ...string) error

	// Sorted Sets - http://redis.io/commands#sorted_set

	ZAdd(key string, score float64, value string) error
	ZIncBy(key string, score float64, value string) error
	ZRem(key string, members ...string) error

	// HyperLogLog

	PFAdd(key string, values ...string) error
	PFCount(key string) error
	PFMerge(mergedKey string, keysToMerge ...string) error
}

type Transactions interface {
	Multi() error
	Exec() ([]interface{}, error)
	// Discard() error
	// Watch(keys ...string) error
	// Unwatch() error
}

type PubSub interface {
}
