package redis

import (
	redigo "github.com/garyburd/redigo/redis"
)

type Pipeline interface {
	NoResultCommands

	receiveAll() ([]interface{}, error)
}

type Transaction interface {
	Pipeline
}

func asTransaction(c *connection) Transaction {
	return &sendOnlyConnection{c: c.c}
}

func asPipeline(c *connection) Pipeline {
	return &sendOnlyConnection{c: c.c}
}

type sendOnlyConnection struct {
	c       redigo.Conn
	counter int
}

// NoResultCommands - Keys

func (s *sendOnlyConnection) Del(keys ...string) error {
	return s.count(s.c.Send("DEL", redigo.Args{}.AddFlat(keys)...))
}

func (s *sendOnlyConnection) Exists(key string) error {
	return s.count(s.c.Send("EXISTS", key))
}

// NoResultCommands - Strings

func (s *sendOnlyConnection) Get(key string) error {
	return s.count(s.c.Send("GET", key))
}

func (s *sendOnlyConnection) Set(key, value string) error {
	return s.count(s.c.Send("SET", key, value))
}

func (s *sendOnlyConnection) SetEx(key, value string, expire int) error {
	return s.count(s.c.Send("SETEX", key, expire, value))
}

// Commands - Hashes

func (s *sendOnlyConnection) HGet(key, field string) error {
	return s.count(s.c.Send("HGET", key, field))
}

func (s *sendOnlyConnection) HSet(key string, field string, value string) error {
	return s.count(s.c.Send("HSET", key, field, value))
}

// NoResultCommands - Lists

func (s *sendOnlyConnection) LPush(key string, values ...string) error {
	return s.count(s.c.Send("LPUSH", redigo.Args{key}.AddFlat(values)...))
}

func (s *sendOnlyConnection) RPush(key string, values ...string) error {
	return s.count(s.c.Send("RPUSH", redigo.Args{key}.AddFlat(values)...))
}

// NoResultCommands - Sets

func (s *sendOnlyConnection) SAdd(key string, members ...string) error {
	return s.count(s.c.Send("SADD", redigo.Args{key}.AddFlat(members)...))
}

// NoResultCommands - Sorted Sets

func (s *sendOnlyConnection) ZAdd(key string, score float64, value string) error {
	return s.count(s.c.Send("ZADD", key, score, value))
}

// NoResultCommands - NoResultCommands

func (s *sendOnlyConnection) ZRem(key string, members ...string) error {
	return s.count(s.c.Send("ZREM", redigo.Args{key}.AddFlat(members)...))
}

// Pipeline - only visible to package

func (s *sendOnlyConnection) receiveAll() ([]interface{}, error) {
	if s.counter == 0 {
		return nil, nil
	}

	replies := make([]interface{}, s.counter)
	for i := 0; i < s.counter; i++ {
		r, err := s.c.Receive()
		if err != nil {
			return nil, err
		}

		replies[i] = r
	}

	s.counter = 0
	return replies, nil
}

// helpers

func (s *sendOnlyConnection) count(err error) error {
	if err != nil {
		return err
	}

	s.counter++
	return nil
}