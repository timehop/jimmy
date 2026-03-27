package redis_test

import (
	"fmt"
	netURL "net/url"
	"testing"

	"github.com/timehop/jimmy/redis"
)

func TestConnection(t *testing.T) {
	// Using an arbitrary password should fallback to using no password
	url := "redis://:foopass@localhost:6379/12"
	parsedURL, _ := netURL.Parse(url)
	c, err := redis.NewConnection(parsedURL)
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	flushDB := func() {
		c.Do("FLUSHDB")
	}

	t.Run("NewConnection", func(t *testing.T) {
		t.Run("server has no auth set", func(t *testing.T) {
			t.Run("should ping without auth", func(t *testing.T) {
				flushDB()
				url := "redis://localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, err := redis.NewConnection(parsedURL)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				_, err = c.Do("PING")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})

			t.Run("should fallback to ping without auth", func(t *testing.T) {
				flushDB()
				url := "redis://user:testpass@localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, err := redis.NewConnection(parsedURL)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				_, err = c.Do("PING")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		t.Run("server requires auth", func(t *testing.T) {
			setupAuth := func() {
				url := "redis://localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)
				c.Do("CONFIG", "SET", "requirepass", "testpass")
			}
			teardownAuth := func() {
				url := "redis://:testpass@localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)
				c.Do("CONFIG", "SET", "requirepass", "")
			}

			t.Run("should fail to ping without auth", func(t *testing.T) {
				flushDB()
				setupAuth()
				t.Cleanup(teardownAuth)

				url := "redis://localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)

				_, err := c.Do("PING")
				if err == nil {
					t.Error("expected error, got nil")
				}
			})

			t.Run("should successfully ping with auth", func(t *testing.T) {
				flushDB()
				setupAuth()
				t.Cleanup(teardownAuth)

				url := "redis://:testpass@localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)

				_, err := c.Do("PING")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})
	})

	t.Run("DEL", func(t *testing.T) {
		t.Run("no key exists returns 0", func(t *testing.T) {
			flushDB()
			i, err := c.Del("doesnotexist")
			if i != 0 {
				t.Errorf("got %d, want 0", i)
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		t.Run("key exists returns 1", func(t *testing.T) {
			flushDB()
			c.Set("exists", "The best leaders know when to follow.")
			i, err := c.Del("exists")
			if i != 1 {
				t.Errorf("got %d, want 1", i)
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	})

	t.Run("TTL", func(t *testing.T) {
		t.Run("without key returns -2", func(t *testing.T) {
			flushDB()
			i, err := c.TTL("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i != -2 {
				t.Errorf("got %d, want -2", i)
			}
		})

		t.Run("key without expiration returns -1", func(t *testing.T) {
			flushDB()
			c.Set("foo", "bar")

			i, err := c.TTL("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i != -1 {
				t.Errorf("got %d, want -1", i)
			}
		})

		t.Run("key with expiration returns ttl", func(t *testing.T) {
			flushDB()
			c.SetEx("biz", "baz", 15)

			i, err := c.TTL("biz")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i != 15 {
				t.Errorf("got %d, want 15", i)
			}
		})
	})

	t.Run("PFAdd", func(t *testing.T) {
		t.Run("should indicate HyperLogLog register was altered", func(t *testing.T) {
			flushDB()
			i, err := c.PFAdd("_tests:jimmy:redis:foo1", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i != 1 {
				t.Errorf("got %d, want 1", i)
			}
		})

		t.Run("should indicate HyperLogLog register was not altered", func(t *testing.T) {
			flushDB()
			_, err := c.PFAdd("_tests:jimmy:redis:foo2", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			i, err := c.PFAdd("_tests:jimmy:redis:foo2", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i != 0 {
				t.Errorf("got %d, want 0", i)
			}
		})
	})

	t.Run("PFCount", func(t *testing.T) {
		t.Run("should return approximate cardinality", func(t *testing.T) {
			flushDB()
			var actualCardinality float64 = 20000
			for i := 0; float64(i) < actualCardinality; i++ {
				_, err := c.PFAdd("_tests:jimmy:redis:foo3", fmt.Sprint(i))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			card, err := c.PFCount("_tests:jimmy:redis:foo3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if float64(card) >= actualCardinality*1.2 {
				t.Errorf("cardinality %d too high (max %v)", card, actualCardinality*1.2)
			}
			if float64(card) <= actualCardinality*0.8 {
				t.Errorf("cardinality %d too low (min %v)", card, actualCardinality*0.8)
			}
		})
	})

	t.Run("PFMerge", func(t *testing.T) {
		t.Run("should return approximate cardinality of union", func(t *testing.T) {
			flushDB()
			setA := []int{1, 2, 3, 4, 5}
			setB := []int{3, 4, 5, 6, 7}
			setC := []int{8, 9, 10, 11, 12}

			for _, x := range setA {
				_, err := c.PFAdd("_tests:jimmy:redis:hll1", fmt.Sprint(x))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			for _, x := range setB {
				_, err := c.PFAdd("_tests:jimmy:redis:hll2", fmt.Sprint(x))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			for _, x := range setC {
				_, err := c.PFAdd("_tests:jimmy:redis:hll3", fmt.Sprint(x))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			for i := 1; i < 4; i++ {
				card, err := c.PFCount(fmt.Sprintf("_tests:jimmy:redis:hll%d", i))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if card != 5 {
					t.Errorf("hll%d: got %d, want 5", i, card)
				}
			}

			ok, err := c.PFMerge("_tests:jimmy:redis:hll1+2", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			card, err := c.PFCount("_tests:jimmy:redis:hll1+2")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if card != 7 {
				t.Errorf("got %d, want 7", card)
			}

			ok, err = c.PFMerge("_tests:jimmy:redis:hll1+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			card, err = c.PFCount("_tests:jimmy:redis:hll1+3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if card != 10 {
				t.Errorf("got %d, want 10", card)
			}

			ok, err = c.PFMerge("_tests:jimmy:redis:hll1+2+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2", "_tests:jimmy:redis:hll3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			card, err = c.PFCount("_tests:jimmy:redis:hll1+2+3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if card != 12 {
				t.Errorf("got %d, want 12", card)
			}
		})
	})

	t.Run("LTrim", func(t *testing.T) {
		t.Run("when a list is trimmed", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:list"

			for i := range 5 {
				c.LPush(key, fmt.Sprint(i))
			}

			size, err := c.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 5 {
				t.Errorf("got %d, want 5", size)
			}

			// Trim nothing
			err = c.LTrim(key, 0, 4)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			size, err = c.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 5 {
				t.Errorf("got %d, want 5", size)
			}

			// Trim first element
			err = c.LTrim(key, 1, 5)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			size, err = c.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 4 {
				t.Errorf("got %d, want 4", size)
			}

			item, err := c.LPop(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if item != "3" {
				t.Errorf("got %q, want %q", item, "3")
			}

			// Trim last element
			err = c.LTrim(key, -4, -1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			size, err = c.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 3 {
				t.Errorf("got %d, want 3", size)
			}

			item, err = c.LPop(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if item != "2" {
				t.Errorf("got %q, want %q", item, "2")
			}
		})

		t.Run("when a not-list is trimmed returns error", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:not-list"

			if err := c.Set(key, "yay"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := c.LTrim(key, 0, 4); err == nil {
				t.Error("expected error, got nil")
			}

			c.Del(key)
			_, err := c.SAdd(key, "yay")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := c.LTrim(key, 0, 4); err == nil {
				t.Error("expected error, got nil")
			}
		})
	})

	t.Run("LRange", func(t *testing.T) {
		t.Run("empty list returns nothing", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:list"
			things, err := c.LRange(key, 0, -1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(things) != 0 {
				t.Errorf("expected empty, got %v", things)
			}
		})

		t.Run("list returns items", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:list"
			for i := range 5 {
				_, err := c.LPush(key, fmt.Sprint(i))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			things, err := c.LRange(key, 0, -1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(things) != 5 {
				t.Errorf("got len %d, want 5", len(things))
			}

			things, err = c.LRange(key, 0, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(things) != 1 {
				t.Errorf("got len %d, want 1", len(things))
			}
			if things[0] != "4" {
				t.Errorf("got %q, want %q", things[0], "4")
			}

			things, err = c.LRange(key, -1, -1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(things) != 1 {
				t.Errorf("got len %d, want 1", len(things))
			}
			if things[0] != "0" {
				t.Errorf("got %q, want %q", things[0], "0")
			}
		})
	})

	t.Run("SMove", func(t *testing.T) {
		t.Run("should move member to other set", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:smove"

			c.SAdd(key+":a", "foobar")

			moved, err := c.SMove(key+":a", key+":b", "foobar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !moved {
				t.Error("expected true, got false")
			}

			members, _ := c.SMembers(key + ":a")
			if len(members) != 0 {
				t.Errorf("got len %d, want 0", len(members))
			}

			members, _ = c.SMembers(key + ":b")
			if len(members) != 1 || members[0] != "foobar" {
				t.Errorf("got %v, want [foobar]", members)
			}
		})
	})

	t.Run("SETNX", func(t *testing.T) {
		t.Run("should not set existing key", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:setnx.existing"
			c.Set(key, "foo")

			ok, err := c.SetNX(key, "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok {
				t.Error("expected false, got true")
			}

			foo, _ := c.Get(key)
			if foo != "foo" {
				t.Errorf("got %q, want %q", foo, "foo")
			}
		})

		t.Run("should set non-existent key", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:setnx.notexisting"

			ok, err := c.SetNX(key, "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			foo, _ := c.Get(key)
			if foo != "bar" {
				t.Errorf("got %q, want %q", foo, "bar")
			}
		})
	})

	t.Run("ZScan", func(t *testing.T) {
		t.Run("should scan the sorted set", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:zscan"

			c.ZAdd(key, 1, "a")
			c.ZAdd(key, 2, "b")
			c.ZAdd(key, 3, "c")
			c.ZAdd(key, 4, "d")
			c.ZAdd(key, 5, "e")

			var scanned []string
			var scannedScores []float64
			var cursor int
			var matches []string
			var scores []float64
			var err error

			cursor, matches, scores, err = c.ZScan(key, cursor, "", 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			scanned = append(scanned, matches...)
			scannedScores = append(scannedScores, scores...)
			for cursor != 0 {
				cursor, matches, scores, err = c.ZScan(key, cursor, "", 1)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				scanned = append(scanned, matches...)
				scannedScores = append(scannedScores, scores...)
			}

			if len(scanned) != 5 {
				t.Errorf("got len %d, want 5", len(scanned))
			}
			for _, want := range []string{"a", "b", "c", "d", "e"} {
				if !containsString(scanned, want) {
					t.Errorf("expected %q in %v", want, scanned)
				}
			}

			expectedScores := map[string]float64{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
			for i, elem := range scanned {
				if scannedScores[i] != expectedScores[elem] {
					t.Errorf("%s: got score %v, want %v", elem, scannedScores[i], expectedScores[elem])
				}
			}
		})
	})

	t.Run("SScan", func(t *testing.T) {
		t.Run("should scan the set", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:sscan"

			c.SAdd(key, "a", "b", "c", "d", "e")

			var scanned []string
			var cursor int
			var matches []string
			var err error

			cursor, matches, err = c.SScan(key, cursor, "", 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			scanned = append(scanned, matches...)
			for cursor != 0 {
				cursor, matches, err = c.SScan(key, cursor, "", 1)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				scanned = append(scanned, matches...)
			}

			if len(scanned) != 5 {
				t.Errorf("got len %d, want 5", len(scanned))
			}
			for _, want := range []string{"a", "b", "c", "d", "e"} {
				if !containsString(scanned, want) {
					t.Errorf("expected %q in %v", want, scanned)
				}
			}
		})
	})

	t.Run("HGet", func(t *testing.T) {
		t.Run("key exists with field returns value", func(t *testing.T) {
			flushDB()
			if _, err := c.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, err := c.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "baz" {
				t.Errorf("got %q, want %q", val, "baz")
			}
		})

		t.Run("key exists without field returns error", func(t *testing.T) {
			flushDB()
			if _, err := c.HSet("foo", "blah", "blech"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, err := c.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}
		})

		t.Run("key does not exist returns error", func(t *testing.T) {
			flushDB()
			val, err := c.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}
		})

		t.Run("key exists but not hash returns error", func(t *testing.T) {
			flushDB()
			if err := c.Set("foo", "yo"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, err := c.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}
		})
	})

	t.Run("HGetAll", func(t *testing.T) {
		t.Run("key exists with 2 pairs returns pairs", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{"bar": "baz", "blah": "blech"}
			err := c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := c.HGetAll("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 2 {
				t.Errorf("got len %d, want 2", len(vals))
			}
			if vals["bar"] != "baz" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "baz")
			}
			if vals["blah"] != "blech" {
				t.Errorf("blah: got %q, want %q", vals["blah"], "blech")
			}
		})

		t.Run("key does not exist returns empty map", func(t *testing.T) {
			flushDB()
			vals, err := c.HGetAll("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 0 {
				t.Errorf("got len %d, want 0", len(vals))
			}
		})
	})

	t.Run("HSet", func(t *testing.T) {
		t.Run("new key returns true", func(t *testing.T) {
			flushDB()
			isNew, err := c.HSet("foo", "bar", "baz")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isNew {
				t.Error("expected true, got false")
			}

			val, err := c.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "baz" {
				t.Errorf("got %q, want %q", val, "baz")
			}
		})

		t.Run("existing key new field returns true", func(t *testing.T) {
			flushDB()
			if _, err := c.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			isNew, err := c.HSet("foo", "yo", "oy")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isNew {
				t.Error("expected true, got false")
			}

			val, err := c.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "baz" {
				t.Errorf("got %q, want %q", val, "baz")
			}

			val, err = c.HGet("foo", "yo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "oy" {
				t.Errorf("got %q, want %q", val, "oy")
			}
		})

		t.Run("existing key existing field returns false", func(t *testing.T) {
			flushDB()
			if _, err := c.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			isNew, err := c.HSet("foo", "bar", "yo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if isNew {
				t.Error("expected false, got true")
			}

			val, err := c.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "yo" {
				t.Errorf("got %q, want %q", val, "yo")
			}
		})

		t.Run("existing key not hash returns error", func(t *testing.T) {
			flushDB()
			if err := c.Set("foo", "bar"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			isNew, err := c.HSet("foo", "bar", "yo")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if isNew {
				t.Error("expected false, got true")
			}

			val, err := c.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}

			val, err = c.Get("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "bar" {
				t.Errorf("got %q, want %q", val, "bar")
			}
		})
	})

	t.Run("HMGet", func(t *testing.T) {
		t.Run("key exists with 2 specified keys", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{"bar": "baz", "blah": "blech"}
			err := c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := c.HMGet("foo", "bar", "blah")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 2 {
				t.Errorf("got len %d, want 2", len(vals))
			}
			if vals["bar"] != "baz" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "baz")
			}
			if vals["blah"] != "blech" {
				t.Errorf("blah: got %q, want %q", vals["blah"], "blech")
			}
		})

		t.Run("key exists with 2 of 3 specified keys", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{"bar": "baz", "blah": "blech"}
			err := c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := c.HMGet("foo", "bar", "yo", "blah")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 3 {
				t.Errorf("got len %d, want 3", len(vals))
			}
			if vals["bar"] != "baz" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "baz")
			}
			if vals["yo"] != "" {
				t.Errorf("yo: got %q, want empty string", vals["yo"])
			}
			if vals["blah"] != "blech" {
				t.Errorf("blah: got %q, want %q", vals["blah"], "blech")
			}
		})

		t.Run("key does not exist", func(t *testing.T) {
			flushDB()
			vals, err := c.HMGet("foo", "bar", "blah")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 2 {
				t.Errorf("got len %d, want 2", len(vals))
			}
			if vals["bar"] != "" {
				t.Errorf("bar: got %q, want empty string", vals["bar"])
			}
			if vals["blah"] != "" {
				t.Errorf("blah: got %q, want empty string", vals["blah"])
			}
		})

		t.Run("no fields returns error", func(t *testing.T) {
			flushDB()
			vals, err := c.HMGet("foo")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if len(vals) != 0 {
				t.Errorf("got len %d, want 0", len(vals))
			}
		})
	})

	t.Run("HMSet", func(t *testing.T) {
		t.Run("new key with 2 string pairs", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{"bar": "baz", "blah": "blech"}
			err := c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := c.HGetAll("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 2 {
				t.Errorf("got len %d, want 2", len(vals))
			}
			if vals["bar"] != "baz" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "baz")
			}
			if vals["blah"] != "blech" {
				t.Errorf("blah: got %q, want %q", vals["blah"], "blech")
			}
		})

		t.Run("new key with 2 int pairs", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{"bar": 18, "blah": 42}
			err := c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := c.HGetAll("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 2 {
				t.Errorf("got len %d, want 2", len(vals))
			}
			if vals["bar"] != "18" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "18")
			}
			if vals["blah"] != "42" {
				t.Errorf("blah: got %q, want %q", vals["blah"], "42")
			}
		})

		t.Run("existing key with 3 pairs update 2", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{"bar": 18, "blah": 42, "yo": "oy"}
			err := c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			in = map[string]interface{}{"bar": "baz", "blah": "blech"}
			err = c.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := c.HGetAll("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 3 {
				t.Errorf("got len %d, want 3", len(vals))
			}
			if vals["bar"] != "baz" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "baz")
			}
			if vals["blah"] != "blech" {
				t.Errorf("blah: got %q, want %q", vals["blah"], "blech")
			}
			if vals["yo"] != "oy" {
				t.Errorf("yo: got %q, want %q", vals["yo"], "oy")
			}
		})

		t.Run("existing key not hash returns error", func(t *testing.T) {
			flushDB()
			if err := c.Set("foo", "bar"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			in := map[string]interface{}{"bar": "baz", "blah": "blech"}
			err := c.HMSet("foo", in)
			if err == nil {
				t.Error("expected error, got nil")
			}

			val, err := c.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}

			val, err = c.Get("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "bar" {
				t.Errorf("got %q, want %q", val, "bar")
			}
		})

		t.Run("existing key empty map returns error", func(t *testing.T) {
			flushDB()
			if _, err := c.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			in := map[string]interface{}{}
			err := c.HMSet("foo", in)
			if err == nil {
				t.Error("expected error, got nil")
			}

			vals, err := c.HGetAll("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(vals) != 1 {
				t.Errorf("got len %d, want 1", len(vals))
			}
			if vals["bar"] != "baz" {
				t.Errorf("bar: got %q, want %q", vals["bar"], "baz")
			}
		})

		t.Run("new key empty map returns error", func(t *testing.T) {
			flushDB()
			in := map[string]interface{}{}
			err := c.HMSet("foo", in)
			if err == nil {
				t.Error("expected error, got nil")
			}

			exists, err := c.Exists("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if exists {
				t.Error("expected false, got true")
			}
		})
	})
}

// containsString checks if a string slice contains a given string.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

