package redisurl_test

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/timehop/jimmy/redis/redisurl"
)

func TestConnect(t *testing.T) {
	c, err := redisurl.Connect()

	if err != nil {
		t.Errorf("Error returned")
	}

	pong, err := redis.String(c.Do("PING"))

	if err != nil {
		t.Errorf("Call to PING returned an error: %v", err)
	}

	if pong != "PONG" {
		t.Errorf("Wanted PONG, got %v\n", pong)
	}
}
