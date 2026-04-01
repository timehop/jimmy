package redisurl_test

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/timehop/jimmy/redis/redisurl"
)

func TestConnect(t *testing.T) {
	c, err := redisurl.Connect()
	if err != nil {
		t.Fatalf("Error returned: %v", err)
	}

	if c == nil {
		t.Fatal("Connection is nil")
	}
	defer c.Close()

	pong, err := redis.String(c.Do("PING"))

	if err != nil {
		t.Errorf("Call to PING returned an error: %v", err)
	}

	if pong != "PONG" {
		t.Errorf("Wanted PONG, got %v\n", pong)
	}
}

func TestConnectToURL_InvalidURL(t *testing.T) {
	c, err := redisurl.ConnectToURL("://invalid url")
	if err == nil {
		t.Error("Expected error for invalid URL, got nil")
	}

	if c != nil {
		t.Error("Expected nil connection for invalid URL")
	}
}

func TestConnectToURL_InvalidHost(t *testing.T) {
	c, err := redisurl.ConnectToURL("redis://nonexistent-host-12345:6379")
	if err == nil {
		t.Error("Expected error for invalid host, got nil")
	}

	// This is the critical check - connection should be nil on error
	if c != nil {
		t.Error("Expected nil connection when dial fails")
	}
}

func TestConnectToURL_WithAuth(t *testing.T) {
	c, err := redisurl.ConnectToURL("redis://:password@localhost:6379/10")
	// When AUTH fails, connection is returned so caller can check error type and retry
	if err != nil {
		if c == nil {
			t.Error("Connection should be returned even when AUTH fails (for fallback logic)")
		} else {
			c.Close() // Caller's responsibility to close on error
		}
	}

	if c != nil && err == nil {
		defer c.Close()
	}
}
