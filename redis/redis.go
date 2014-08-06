package redis

import (
	redigo "github.com/garyburd/redigo/redis"
)

var ErrNil = redigo.ErrNil

type Commands interface {

	// Keys - http://redis.io/commands#generic

	Del(keys ...string) (int, error)
	Exists(key string) (bool, error)

	// Strings - http://redis.io/commands#string

	Get(key string) (string, error)
	Set(key, value string) error
	SetEx(key, value string, expire int) error

	// Hashes - http://redis.io/commands#hash

	HGet(key string, field string) (string, error)
	HSet(key string, field string, value string) (isNew bool, err error)

	// Lists - http://redis.io/commands#list

	BLPop(timeout int, keys ...string) (listName string, value string, err error)
	BRPop(timeout int, keys ...string) (listName string, value string, err error)
	LLen(key string) (int, error)
	LPop(key string) (string, error)
	LPush(key string, values ...string) (int, error)
	RPush(key string, values ...string) (int, error)

	// Sets - http://redis.io/commands#generic

	SAdd(key string, members ...string) (int, error)
	SCard(key string) (int, error)

	// Sorted Sets - http://redis.io/commands#sorted_set

	ZAdd(key string, score float64, value string) (int, error)
	ZCard(key string) (int, error)
	ZRangeByScore(key, start, stop string) ([]string, error)
	ZRangeByScoreWithLimit(key, start, stop string, offset, count int) ([]string, error)
	ZRem(key string, members ...string) (removed int, err error)
}

type NoResultCommands interface {

	// Keys - http://redis.io/commands#generic

	Del(keys ...string) error
	Exists(key string) error

	// Strings - http://redis.io/commands#string

	Get(key string) error
	Set(key, value string) error
	SetEx(key, value string, expire int) error

	// Hashes - http://redis.io/commands#hash

	HGet(key string, field string) error
	HSet(key string, field string, value string) error

	// Lists - http://redis.io/commands#list

	LPush(key string, values ...string) error
	RPush(key string, values ...string) error

	// Sets - http://redis.io/commands#generic

	SAdd(key string, members ...string) error

	// Sorted Sets - http://redis.io/commands#sorted_set

	ZAdd(key string, score float64, value string) error
	ZRem(key string, members ...string) error
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
