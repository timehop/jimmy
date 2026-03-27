package redis_test

import (
	"fmt"
	"testing"

	"github.com/timehop/jimmy/redis"
)

func TestPool(t *testing.T) {
	redisURL := "redis://:foopass@localhost:6379/10"
	p, err := redis.NewPool(redisURL, redis.DefaultConfig)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	flushDB := func() {
		p.Do(func(c redis.Connection) { c.Do("FLUSHDB") })
	}

	t.Run("NewPool", func(t *testing.T) {
		t.Run("server has no auth set", func(t *testing.T) {
			t.Run("should ping without auth", func(t *testing.T) {
				flushDB()
				p, _ := redis.NewPool("redis://localhost:6379", redis.DefaultConfig)
				if p == nil {
					t.Fatal("expected non-nil pool")
				}

				c, err := p.GetConnection()
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
				p, _ := redis.NewPool("redis://user:testpass@localhost:6379", redis.DefaultConfig)

				c, err := p.GetConnection()
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
				p, _ := redis.NewPool("redis://localhost:6379", redis.DefaultConfig)
				c, _ := p.GetConnection()
				c.Do("CONFIG", "SET", "requirepass", "testpass")
			}
			teardownAuth := func() {
				p, _ := redis.NewPool("redis://:testpass@localhost:6379", redis.DefaultConfig)
				c, _ := p.GetConnection()
				c.Do("CONFIG", "SET", "requirepass", "")
			}

			t.Run("should fail to ping without auth", func(t *testing.T) {
				flushDB()
				setupAuth()
				t.Cleanup(teardownAuth)

				p, _ := redis.NewPool("redis://localhost:6379", redis.DefaultConfig)
				c, _ := p.GetConnection()
				_, err := c.Do("PING")
				if err == nil {
					t.Error("expected error, got nil")
				}
			})

			t.Run("should successfully ping with auth", func(t *testing.T) {
				flushDB()
				setupAuth()
				t.Cleanup(teardownAuth)

				p, _ := redis.NewPool("redis://user:testpass@localhost:6379", redis.DefaultConfig)
				c, _ := p.GetConnection()
				_, err := c.Do("PING")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})
	})

	t.Run("PFAdd", func(t *testing.T) {
		t.Run("should indicate HyperLogLog register was altered", func(t *testing.T) {
			flushDB()
			i, err := p.PFAdd("_tests:jimmy:redis:foo1", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i != 1 {
				t.Errorf("got %d, want 1", i)
			}
		})

		t.Run("should indicate HyperLogLog register was not altered", func(t *testing.T) {
			flushDB()
			_, err := p.PFAdd("_tests:jimmy:redis:foo2", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			i, err := p.PFAdd("_tests:jimmy:redis:foo2", "bar")
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
				_, err := p.PFAdd("_tests:jimmy:redis:foo3", fmt.Sprint(i))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			card, err := p.PFCount("_tests:jimmy:redis:foo3")
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
				_, err := p.PFAdd("_tests:jimmy:redis:hll1", fmt.Sprint(x))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			for _, x := range setB {
				_, err := p.PFAdd("_tests:jimmy:redis:hll2", fmt.Sprint(x))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			for _, x := range setC {
				_, err := p.PFAdd("_tests:jimmy:redis:hll3", fmt.Sprint(x))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			for i := 1; i < 4; i++ {
				card, err := p.PFCount(fmt.Sprintf("_tests:jimmy:redis:hll%d", i))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if card != 5 {
					t.Errorf("hll%d: got %d, want 5", i, card)
				}
			}

			ok, err := p.PFMerge("_tests:jimmy:redis:hll1+2", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			card, err := p.PFCount("_tests:jimmy:redis:hll1+2")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if card != 7 {
				t.Errorf("got %d, want 7", card)
			}

			ok, err = p.PFMerge("_tests:jimmy:redis:hll1+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			card, err = p.PFCount("_tests:jimmy:redis:hll1+3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if card != 10 {
				t.Errorf("got %d, want 10", card)
			}

			ok, err = p.PFMerge("_tests:jimmy:redis:hll1+2+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2", "_tests:jimmy:redis:hll3")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Error("expected true, got false")
			}

			card, err = p.PFCount("_tests:jimmy:redis:hll1+2+3")
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
				p.LPush(key, fmt.Sprint(i))
			}

			size, err := p.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 5 {
				t.Errorf("got %d, want 5", size)
			}

			// Trim nothing
			err = p.LTrim(key, 0, 4)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			size, err = p.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 5 {
				t.Errorf("got %d, want 5", size)
			}

			// Trim first element
			err = p.LTrim(key, 1, 5)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			size, err = p.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 4 {
				t.Errorf("got %d, want 4", size)
			}

			item, err := p.LPop(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if item != "3" {
				t.Errorf("got %q, want %q", item, "3")
			}

			// Trim last element
			err = p.LTrim(key, -4, -1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			size, err = p.LLen(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != 3 {
				t.Errorf("got %d, want 3", size)
			}

			item, err = p.LPop(key)
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

			if err := p.Set(key, "yay"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := p.LTrim(key, 0, 4); err == nil {
				t.Error("expected error, got nil")
			}

			p.Del(key)
			_, err := p.SAdd(key, "yay")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := p.LTrim(key, 0, 4); err == nil {
				t.Error("expected error, got nil")
			}
		})
	})

	t.Run("LRange", func(t *testing.T) {
		t.Run("empty list returns nothing", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:list"
			things, err := p.LRange(key, 0, -1)
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
				_, err := p.LPush(key, fmt.Sprint(i))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			things, err := p.LRange(key, 0, -1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(things) != 5 {
				t.Errorf("got len %d, want 5", len(things))
			}

			things, err = p.LRange(key, 0, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(things) != 1 {
				t.Errorf("got len %d, want 1", len(things))
			}
			if things[0] != "4" {
				t.Errorf("got %q, want %q", things[0], "4")
			}

			things, err = p.LRange(key, -1, -1)
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

			p.SAdd(key+":a", "foobar")

			moved, err := p.SMove(key+":a", key+":b", "foobar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !moved {
				t.Error("expected true, got false")
			}

			members, _ := p.SMembers(key + ":a")
			if len(members) != 0 {
				t.Errorf("got len %d, want 0", len(members))
			}

			members, _ = p.SMembers(key + ":b")
			if len(members) != 1 || members[0] != "foobar" {
				t.Errorf("got %v, want [foobar]", members)
			}
		})
	})

	t.Run("SScan", func(t *testing.T) {
		t.Run("should scan the set", func(t *testing.T) {
			flushDB()
			key := "_tests:jimmy:redis:sscan"

			p.SAdd(key, "a", "b", "c", "d", "e")

			var scanned []string
			var cursor int
			var matches []string
			var err error

			cursor, matches, err = p.SScan(key, cursor, "", 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			scanned = append(scanned, matches...)
			for cursor != 0 {
				cursor, matches, err = p.SScan(key, cursor, "", 1)
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
			if _, err := p.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, err := p.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "baz" {
				t.Errorf("got %q, want %q", val, "baz")
			}
		})

		t.Run("key exists without field returns error", func(t *testing.T) {
			flushDB()
			if _, err := p.HSet("foo", "blah", "blech"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, err := p.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}
		})

		t.Run("key does not exist returns error", func(t *testing.T) {
			flushDB()
			val, err := p.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}
		})

		t.Run("key exists but not hash returns error", func(t *testing.T) {
			flushDB()
			if err := p.Set("foo", "yo"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, err := p.HGet("foo", "bar")
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
			err := p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := p.HGetAll("foo")
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
			vals, err := p.HGetAll("foo")
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
			isNew, err := p.HSet("foo", "bar", "baz")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isNew {
				t.Error("expected true, got false")
			}

			val, err := p.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "baz" {
				t.Errorf("got %q, want %q", val, "baz")
			}
		})

		t.Run("existing key new field returns true", func(t *testing.T) {
			flushDB()
			if _, err := p.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			isNew, err := p.HSet("foo", "yo", "oy")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isNew {
				t.Error("expected true, got false")
			}

			val, err := p.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "baz" {
				t.Errorf("got %q, want %q", val, "baz")
			}

			val, err = p.HGet("foo", "yo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "oy" {
				t.Errorf("got %q, want %q", val, "oy")
			}
		})

		t.Run("existing key existing field returns false", func(t *testing.T) {
			flushDB()
			if _, err := p.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			isNew, err := p.HSet("foo", "bar", "yo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if isNew {
				t.Error("expected false, got true")
			}

			val, err := p.HGet("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "yo" {
				t.Errorf("got %q, want %q", val, "yo")
			}
		})

		t.Run("existing key not hash returns error", func(t *testing.T) {
			flushDB()
			if err := p.Set("foo", "bar"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			isNew, err := p.HSet("foo", "bar", "yo")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if isNew {
				t.Error("expected false, got true")
			}

			val, err := p.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}

			val, err = p.Get("foo")
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
			err := p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := p.HMGet("foo", "bar", "blah")
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
			err := p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := p.HMGet("foo", "bar", "yo", "blah")
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
			vals, err := p.HMGet("foo", "bar", "blah")
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
			vals, err := p.HMGet("foo")
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
			err := p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := p.HGetAll("foo")
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
			err := p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := p.HGetAll("foo")
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
			err := p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			in = map[string]interface{}{"bar": "baz", "blah": "blech"}
			err = p.HMSet("foo", in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vals, err := p.HGetAll("foo")
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
			if err := p.Set("foo", "bar"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			in := map[string]interface{}{"bar": "baz", "blah": "blech"}
			err := p.HMSet("foo", in)
			if err == nil {
				t.Error("expected error, got nil")
			}

			val, err := p.HGet("foo", "bar")
			if err == nil {
				t.Error("expected error, got nil")
			}
			if val != "" {
				t.Errorf("got %q, want empty string", val)
			}

			val, err = p.Get("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != "bar" {
				t.Errorf("got %q, want %q", val, "bar")
			}
		})

		t.Run("existing key empty map returns error", func(t *testing.T) {
			flushDB()
			if _, err := p.HSet("foo", "bar", "baz"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			in := map[string]interface{}{}
			err := p.HMSet("foo", in)
			if err == nil {
				t.Error("expected error, got nil")
			}

			vals, err := p.HGetAll("foo")
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
			err := p.HMSet("foo", in)
			if err == nil {
				t.Error("expected error, got nil")
			}

			exists, err := p.Exists("foo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if exists {
				t.Error("expected false, got true")
			}
		})
	})

	t.Run("ZAdd", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			flushDB()
			added, err := p.ZAdd("foo", 0.123, "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if added != 1 {
				t.Errorf("got %d, want 1", added)
			}
		})
	})

	t.Run("ZRank", func(t *testing.T) {
		t.Run("key exists returns rank", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			rank, err := p.ZRank("foo", "bar")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rank != 0 {
				t.Errorf("got %d, want 0", rank)
			}
			rank, err = p.ZRank("foo", "barfu")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rank != 1 {
				t.Errorf("got %d, want 1", rank)
			}
		})
	})

	t.Run("ZRemRangeByRank", func(t *testing.T) {
		t.Run("removes members with lower or equal rank", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			rank, err := p.ZRank("foo", "barfu")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rank != 1 {
				t.Errorf("got %d, want 1", rank)
			}
			total, err := p.ZRemRangeByRank("foo", 0, 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if total != 2 {
				t.Errorf("got %d, want 2", total)
			}
			rank, err = p.ZRank("foo", "barfoo")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rank != 0 {
				t.Errorf("got %d, want 0", rank)
			}
		})
	})

	t.Run("ZRange", func(t *testing.T) {
		t.Run("returns elements by range", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRange("foo", 1, 2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(values) != 2 {
				t.Fatalf("got len %d, want 2", len(values))
			}
			if values[0] != "barfu" {
				t.Errorf("got %q, want %q", values[0], "barfu")
			}
			if values[1] != "barfoo" {
				t.Errorf("got %q, want %q", values[1], "barfoo")
			}
		})
	})

	t.Run("ZRangeWithScores", func(t *testing.T) {
		t.Run("returns elements with scores", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeWithScores("foo", 1, 2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(values) != 2 {
				t.Fatalf("got len %d, want 2", len(values))
			}
			if values[0].Value != "barfu" {
				t.Errorf("got %q, want %q", values[0].Value, "barfu")
			}
			if values[0].Score != 0.127 {
				t.Errorf("got %v, want %v", values[0].Score, 0.127)
			}
			if values[1].Value != "barfoo" {
				t.Errorf("got %q, want %q", values[1].Value, "barfoo")
			}
			if values[1].Score != 0.132 {
				t.Errorf("got %v, want %v", values[1].Score, 0.132)
			}
		})
	})

	t.Run("ZRangeByScore", func(t *testing.T) {
		t.Run("returns elements by score range", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScore("foo", "(0.123", "0.132")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(values) != 2 {
				t.Fatalf("got len %d, want 2", len(values))
			}
			if values[0] != "barfu" {
				t.Errorf("got %q, want %q", values[0], "barfu")
			}
			if values[1] != "barfoo" {
				t.Errorf("got %q, want %q", values[1], "barfoo")
			}
		})
	})

	t.Run("ZRangeByScoreWithScores", func(t *testing.T) {
		t.Run("returns elements with scores by range", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScoreWithScores("foo", "(0.123", "0.132")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(values) != 2 {
				t.Fatalf("got len %d, want 2", len(values))
			}
			if values[0].Value != "barfu" {
				t.Errorf("got %q, want %q", values[0].Value, "barfu")
			}
			if values[0].Score != 0.127 {
				t.Errorf("got %v, want %v", values[0].Score, 0.127)
			}
			if values[1].Value != "barfoo" {
				t.Errorf("got %q, want %q", values[1].Value, "barfoo")
			}
			if values[1].Score != 0.132 {
				t.Errorf("got %v, want %v", values[1].Score, 0.132)
			}
		})
	})

	t.Run("ZRangeByScoreWithLimit", func(t *testing.T) {
		t.Run("returns limited elements", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScoreWithLimit("foo", "(0.123", "0.132", 1, 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(values) != 1 {
				t.Fatalf("got len %d, want 1", len(values))
			}
			if values[0] != "barfoo" {
				t.Errorf("got %q, want %q", values[0], "barfoo")
			}
		})
	})

	t.Run("ZRangeByScoreWithScoresWithLimit", func(t *testing.T) {
		t.Run("returns limited elements with scores", func(t *testing.T) {
			flushDB()
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScoreWithScoresWithLimit("foo", "(0.123", "0.132", 1, 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(values) != 1 {
				t.Fatalf("got len %d, want 1", len(values))
			}
			if values[0].Value != "barfoo" {
				t.Errorf("got %q, want %q", values[0].Value, "barfoo")
			}
			if values[0].Score != 0.132 {
				t.Errorf("got %v, want %v", values[0].Score, 0.132)
			}
		})
	})
}
