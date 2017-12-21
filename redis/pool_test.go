package redis_test

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/timehop/jimmy/redis"
)

var _ = Describe("Pool", func() {

	redisURL := "redis://:foopass@localhost:6379/10"
	p, err := redis.NewPool(redisURL, redis.DefaultConfig)
	if err != nil {
		panic(err)
	}

	BeforeEach(func() {
		p.Do(func(c redis.Connection) { c.Do("FLUSHDB") })
	})

	Describe("NewPool", func() {
		// Assumes redis' default state is auth-less

		Context("server has no auth set", func() {
			It("should ping without auth", func() {
				p, _ := redis.NewPool("redis://localhost:6379", redis.DefaultConfig)
				Expect(p).ToNot(BeNil())

				c, err := p.GetConnection()
				Expect(err).To(BeNil())
				_, err = c.Do("PING")
				Expect(err).To(BeNil())
			})
			It("should fallback to ping without auth", func() {
				p, _ := redis.NewPool("redis://user:testpass@localhost:6379", redis.DefaultConfig)

				c, err := p.GetConnection()
				Expect(err).To(BeNil())
				_, err = c.Do("PING")
				Expect(err).To(BeNil())
			})
		})

		Context("server requires auth", func() {
			BeforeEach(func() {
				p, _ := redis.NewPool("redis://localhost:6379", redis.DefaultConfig)

				c, _ := p.GetConnection()
				c.Do("CONFIG", "SET", "requirepass", "testpass")
			})
			AfterEach(func() {
				p, _ := redis.NewPool("redis://:testpass@localhost:6379", redis.DefaultConfig)

				c, _ := p.GetConnection()
				c.Do("CONFIG", "SET", "requirepass", "")
			})

			It("should fail to ping without auth", func() {
				p, _ := redis.NewPool("redis://localhost:6379", redis.DefaultConfig)

				c, _ := p.GetConnection()
				_, err := c.Do("PING")
				Expect(err).ToNot(BeNil())
			})
			It("should successfully ping with auth", func() {
				p, _ := redis.NewPool("redis://user:testpass@localhost:6379", redis.DefaultConfig)

				c, _ := p.GetConnection()
				_, err := c.Do("PING")
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("PFAdd", func() {
		It("Should indicate HyperLogLog register was altered (ie: 1)", func() {
			i, err := p.PFAdd("_tests:jimmy:redis:foo1", "bar")
			Expect(err).To(BeNil())
			Expect(i).To(Equal(1))
		})
		It("Should indicate HyperLogLog register was not altered (ie: 0)", func() {
			_, err := p.PFAdd("_tests:jimmy:redis:foo2", "bar")
			Expect(err).To(BeNil())
			i, err := p.PFAdd("_tests:jimmy:redis:foo2", "bar")
			Expect(err).To(BeNil())
			Expect(i).To(Equal(0))
		})
	})

	Describe("PFCount", func() {
		It("Should return the approximate cardinality of the HLL", func() {
			var actualCardinality float64 = 20000
			for i := 0; float64(i) < actualCardinality; i++ {
				_, err := p.PFAdd("_tests:jimmy:redis:foo3", fmt.Sprint(i))
				Expect(err).To(BeNil())
			}
			card, err := p.PFCount("_tests:jimmy:redis:foo3")
			Expect(err).To(BeNil())
			// Check a VERY rough 20% accuracy
			Expect(float64(card)).To(BeNumerically("<", actualCardinality*1.2))
			Expect(float64(card)).To(BeNumerically(">", actualCardinality*(1-0.2)))
		})
	})

	Describe("PFMerge", func() {
		It("Should return the approximate cardinality of the union of multiple HLLs", func() {
			setA := []int{1, 2, 3, 4, 5}
			setB := []int{3, 4, 5, 6, 7}
			setC := []int{8, 9, 10, 11, 12}

			for _, x := range setA {
				_, err := p.PFAdd("_tests:jimmy:redis:hll1", fmt.Sprint(x))
				Expect(err).To(BeNil())
			}

			for _, x := range setB {
				_, err := p.PFAdd("_tests:jimmy:redis:hll2", fmt.Sprint(x))
				Expect(err).To(BeNil())
			}

			for _, x := range setC {
				_, err := p.PFAdd("_tests:jimmy:redis:hll3", fmt.Sprint(x))
				Expect(err).To(BeNil())
			}

			for i := 1; i < 4; i++ {
				card, err := p.PFCount(fmt.Sprintf("_tests:jimmy:redis:hll%d", i))
				Expect(err).To(BeNil())
				Expect(card).To(Equal(5))
			}

			ok, err := p.PFMerge("_tests:jimmy:redis:hll1+2", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())

			card, err := p.PFCount("_tests:jimmy:redis:hll1+2")
			Expect(err).To(BeNil())
			Expect(card).To(Equal(7))

			ok, err = p.PFMerge("_tests:jimmy:redis:hll1+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll3")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())

			card, err = p.PFCount("_tests:jimmy:redis:hll1+3")
			Expect(err).To(BeNil())
			Expect(card).To(Equal(10))

			ok, err = p.PFMerge("_tests:jimmy:redis:hll1+2+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2", "_tests:jimmy:redis:hll3")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())

			card, err = p.PFCount("_tests:jimmy:redis:hll1+2+3")
			Expect(err).To(BeNil())
			Expect(card).To(Equal(12))
		})
	})

	Describe("LTrim", func() {
		Context("When a list is trimmed", func() {
			It("Trims the list", func() {
				key := "_tests:jimmy:redis:list"

				for i := 0; i < 5; i++ {
					p.LPush(key, fmt.Sprint(i))
				}

				size, err := p.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(5))

				// Trim nothing
				err = p.LTrim(key, 0, 4)
				Expect(err).To(BeNil())

				size, err = p.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(5))

				// Trim first element
				err = p.LTrim(key, 1, 5)
				Expect(err).To(BeNil())

				size, err = p.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(4))

				item, err := p.LPop(key)
				Expect(err).To(BeNil())
				Expect(item).To(Equal("3"))

				// Trim last element
				err = p.LTrim(key, -4, -1)
				Expect(err).To(BeNil())

				size, err = p.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(3))

				item, err = p.LPop(key)
				Expect(err).To(BeNil())
				Expect(item).To(Equal("2"))
			})
		})

		Context("When a not-list is trimmed", func() {
			It("Returns an error", func() {
				key := "_tests:jimmy:redis:not-list"

				Expect(p.Set(key, "yay")).To(BeNil())
				Expect(p.LTrim(key, 0, 4)).ToNot(BeNil())

				p.Del(key)
				_, err := p.SAdd(key, "yay")
				Expect(err).To(BeNil())
				Expect(p.LTrim(key, 0, 4)).ToNot(BeNil())
			})
		})
	})

	Describe("LRange", func() {
		Context("When an empty list is ranged", func() {
			It("Returns nothing, but no err", func() {
				key := "_tests:jimmy:redis:list"
				things, err := p.LRange(key, 0, -1)
				Expect(err).To(BeNil())
				Expect(things).To(BeEmpty())
			})
		})

		Context("When a list is ranged", func() {
			It("Returns the items", func() {
				key := "_tests:jimmy:redis:list"
				for i := 0; i < 5; i++ {
					_, err := p.LPush(key, fmt.Sprint(i))
					Expect(err).To(BeNil())
				}

				things, err := p.LRange(key, 0, -1)
				Expect(err).To(BeNil())
				Expect(len(things)).To(Equal(5))

				things, err = p.LRange(key, 0, 0)
				Expect(err).To(BeNil())
				Expect(len(things)).To(Equal(1))
				Expect(things[0]).To(Equal("4"))

				things, err = p.LRange(key, -1, -1)
				Expect(err).To(BeNil())
				Expect(len(things)).To(Equal(1))
				Expect(things[0]).To(Equal("0"))
			})
		})
	})

	Describe("SMove", func() {
		It("Should move the member to the other set", func() {
			key := "_tests:jimmy:redis:smove"

			p.SAdd(key+":a", "foobar")

			moved, err := p.SMove(key+":a", key+":b", "foobar")
			Expect(err).To(BeNil())
			Expect(moved).To(BeTrue())

			members, _ := p.SMembers(key + ":a")
			Expect(len(members)).To(Equal(0))

			members, _ = p.SMembers(key + ":b")
			Expect(members).To(Equal([]string{"foobar"}))
		})
	})

	Describe("SScan", func() {
		It("Should scan the set", func() {
			key := "_tests:jimmy:redis:sscan"

			p.SAdd(key, "a", "b", "c", "d", "e")

			var scanned []string
			var cursor int
			var matches []string
			var err error

			cursor, matches, err = p.SScan(key, cursor, "", 1)
			Expect(err).To(BeNil())
			scanned = append(scanned, matches...)
			for cursor != 0 {
				cursor, matches, err = p.SScan(key, cursor, "", 1)
				Expect(err).To(BeNil())
				scanned = append(scanned, matches...)
			}

			Expect(len(scanned)).To(Equal(5))
			Expect(scanned).To(ContainElement("a"))
			Expect(scanned).To(ContainElement("b"))
			Expect(scanned).To(ContainElement("c"))
			Expect(scanned).To(ContainElement("d"))
			Expect(scanned).To(ContainElement("e"))
		})
	})

	Describe("HGet", func() {
		Context("a key that exists and contains a hash that contains the requested field with a value", func() {
			It("returns the value of that field of that key", func() {
				mustSucceed2(p.HSet("foo", "bar", "baz"))
				val, err := p.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("baz"))
			})
		})

		Context("a key that exists and contains a hash that doesn’t contain the requested field", func() {
			It("returns an error and an empty string", func() {
				mustSucceed2(p.HSet("foo", "blah", "blech"))
				val, err := p.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))
			})
		})

		Context("a key that doesn’t exist", func() {
			It("returns an error and an empty string", func() {
				val, err := p.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))
			})
		})

		Context("a key that exists but doesn’t contain a hash", func() {
			It("returns an error and an empty string", func() {
				mustSucceed1(p.Set("foo", "yo"))
				val, err := p.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))
			})
		})
	})

	Describe("HGetAll", func() {
		Context("a key that exists and contains 2 key/value pairs", func() {
			It("returns the 2 pairs and no error", func() {
				in := map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err := p.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := p.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
			})
		})

		Context("a key that doesn’t exist", func() {
			It("returns an empty map and no error", func() {
				vals, err := p.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(0))
			})
		})
	})

	Describe("HSet", func() {
		Context("a key that doesn’t already exist and two strings", func() {
			It("returns true and nil and contain the new pair", func() {
				isNew, err := p.HSet("foo", "bar", "baz")
				Expect(err).To(BeNil())
				Expect(isNew).To(Equal(true))

				val, err := p.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("baz"))
			})
		})

		Context("a key that already exists and a field that it doesn’t already contain", func() {
			It("returns true and nil and contain both fields", func() {
				mustSucceed2(p.HSet("foo", "bar", "baz"))
				isNew, err := p.HSet("foo", "yo", "oy")
				Expect(err).To(BeNil())
				Expect(isNew).To(Equal(true))

				val, err := p.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("baz"))

				val, err = p.HGet("foo", "yo")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("oy"))
			})
		})

		Context("a key that already exists and a field that it already contains", func() {
			It("returns false and nil and change the value of the field", func() {
				mustSucceed2(p.HSet("foo", "bar", "baz"))
				isNew, err := p.HSet("foo", "bar", "yo")
				Expect(err).To(BeNil())
				Expect(isNew).To(Equal(false))

				val, err := p.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("yo"))
			})
		})

		Context("a key that already exists and is not a hash", func() {
			It("returns false and an error", func() {
				mustSucceed1(p.Set("foo", "bar"))
				isNew, err := p.HSet("foo", "bar", "yo")
				Expect(err).ToNot(BeNil())
				Expect(isNew).To(Equal(false))

				val, err := p.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))

				val, err = p.Get("foo")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("bar"))
			})
		})
	})

	Describe("HMGet", func() {
		Context("a key that exists and contains the 2 specified keys", func() {
			It("returns the 2 pairs and no error", func() {
				in := map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err := p.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := p.HMGet("foo", "bar", "blah")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
			})
		})

		Context("a key that exists and contains the 2 of the 3 specified keys", func() {
			It("returns the 2 pairs and no error", func() {
				in := map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err := p.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := p.HMGet("foo", "bar", "yo", "blah")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(3))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("yo", ""))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
			})
		})

		Context("a key that doesn’t exist", func() {
			It("returns nil values and no error", func() {
				vals, err := p.HMGet("foo", "bar", "blah")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", ""))
				Expect(vals).To(HaveKeyWithValue("blah", ""))
			})
		})

		Context("no fields", func() {
			It("returns nil values and an error", func() {
				vals, err := p.HMGet("foo")
				Expect(err).ToNot(BeNil())
				Expect(vals).To(HaveLen(0))
			})
		})
	})

	Describe("HMSet", func() {
		Context("a key that doesn’t already exist and a map with 2 string pairs", func() {
			It("returns nil and creates the hash containing the new pairs", func() {
				in := map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err := p.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := p.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
			})
		})

		Context("a key that doesn’t already exist and a map with 2 int pairs", func() {
			It("returns nil and creates the hash containing the new pairs", func() {
				in := map[string]interface{}{
					"bar":  18,
					"blah": 42,
				}
				err := p.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := p.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", "18"))
				Expect(vals).To(HaveKeyWithValue("blah", "42"))
			})
		})

		Context("a key that already exists with 3 pairs and a map with 2 pairs that it already contains", func() {
			It("returns nil and changes the hash to contain the fields with their new values, but leaves other fields alone", func() {
				in := map[string]interface{}{
					"bar":  18,
					"blah": 42,
					"yo":   "oy",
				}
				err := p.HMSet("foo", in)
				Expect(err).To(BeNil())

				in = map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err = p.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := p.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(3))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
				Expect(vals).To(HaveKeyWithValue("yo", "oy"))
			})
		})

		Context("a key that already exists and is not a hash", func() {
			It("returns false and an error", func() {
				mustSucceed1(p.Set("foo", "bar"))

				in := map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err := p.HMSet("foo", in)
				Expect(err).ToNot(BeNil())

				val, err := p.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))

				val, err = p.Get("foo")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("bar"))
			})
		})

		Context("a key that already exists and an empty map", func() {
			It("returns an error and doesn’t change existing the key", func() {
				mustSucceed2(p.HSet("foo", "bar", "baz"))
				in := map[string]interface{}{}
				err := p.HMSet("foo", in)
				Expect(err).ToNot(BeNil())

				vals, err := p.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(1))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
			})
		})

		Context("a key that doesn’t already exist and an empty map", func() {
			It("returns an error and doesn’t create the key", func() {
				in := map[string]interface{}{}
				err := p.HMSet("foo", in)
				Expect(err).ToNot(BeNil())

				exists, err := p.Exists("foo")
				Expect(err).To(BeNil())
				Expect(exists).To(BeFalse())
			})
		})
	})

	Describe("ZAdd", func() {
		Context("happy path", func() {
			It("succeeds", func() {
				added, err := p.ZAdd("foo", 0.123, "bar")
				Expect(err).To(BeNil())
				Expect(added).To(Equal(1))
			})
		})
	})

	Describe("ZRank", func() {
		Context("a key that exists", func() {
			It("returns a rank", func() {
				p.ZAdd("foo", 0.123, "bar")
				p.ZAdd("foo", 0.127, "barfu")
				rank, err := p.ZRank("foo", "bar")
				Expect(err).To(BeNil())
				Expect(rank).To(Equal(0))
				rank, err = p.ZRank("foo", "barfu")
				Expect(err).To(BeNil())
				Expect(rank).To(Equal(1))
			})
		})
	})

	Describe("ZRemRangeByRank", func() {
		Context("the rank of a member", func() {
			It("removes members with lower or equal rank", func() {
				p.ZAdd("foo", 0.123, "bar")
				p.ZAdd("foo", 0.127, "barfu")
				p.ZAdd("foo", 0.132, "barfoo")
				rank, err := p.ZRank("foo", "barfu")
				Expect(err).To(BeNil())
				Expect(rank).To(Equal(1))
				total, err := p.ZRemRangeByRank("foo", 0, 1)
				Expect(err).To(BeNil())
				Expect(total).To(Equal(2))
				rank, err = p.ZRank("foo", "barfoo")
				Expect(err).To(BeNil())
				Expect(rank).To(Equal(0))
			})
		})
	})

	Describe("ZRange", func() {
		It("returns all elements by range", func() {
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRange("foo", 1, 2)
			Expect(err).To(BeNil())
			Expect(values).To(HaveLen(2))
			Expect(values[0]).To(Equal("barfu"))
			Expect(values[1]).To(Equal("barfoo"))
		})
	})

	Describe("ZRangeWithScores", func() {
		It("returns all elements by range", func() {
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeWithScores("foo", 1, 2)
			Expect(err).To(BeNil())
			Expect(values).To(HaveLen(2))
			Expect(values[0].Value).To(Equal("barfu"))
			Expect(values[0].Score).To(Equal(0.127))
			Expect(values[1].Value).To(Equal("barfoo"))
			Expect(values[1].Score).To(Equal(0.132))
		})
	})

	Describe("ZRangeByScore", func() {
		It("returns all elements by range", func() {
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScore("foo", "(0.123", "0.132")
			Expect(err).To(BeNil())
			Expect(values).To(HaveLen(2))
			Expect(values[0]).To(Equal("barfu"))
			Expect(values[1]).To(Equal("barfoo"))
		})
	})

	Describe("ZRangeByScoreWithScores", func() {
		It("returns all elements by range", func() {
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScoreWithScores("foo", "(0.123", "0.132")
			Expect(err).To(BeNil())
			Expect(values).To(HaveLen(2))
			Expect(values[0].Value).To(Equal("barfu"))
			Expect(values[0].Score).To(Equal(0.127))
			Expect(values[1].Value).To(Equal("barfoo"))
			Expect(values[1].Score).To(Equal(0.132))
		})
	})

	Describe("ZRangeByScoreWithLimit", func() {
		It("returns all elements by range", func() {
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScoreWithLimit("foo", "(0.123", "0.132", 1, 1)
			Expect(err).To(BeNil())
			Expect(values).To(HaveLen(1))
			Expect(values[0]).To(Equal("barfoo"))
		})
	})

	Describe("ZRangeByScoreWithScoresWithLimit", func() {
		It("returns all elements by range", func() {
			p.ZAdd("foo", 0.123, "bar")
			p.ZAdd("foo", 0.127, "barfu")
			p.ZAdd("foo", 0.132, "barfoo")
			p.ZAdd("foo", 0.133, "barfubar")
			values, err := p.ZRangeByScoreWithScoresWithLimit("foo", "(0.123", "0.132", 1, 1)
			Expect(err).To(BeNil())
			Expect(values).To(HaveLen(1))
			Expect(values[0].Value).To(Equal("barfoo"))
			Expect(values[0].Score).To(Equal(0.132))
		})
	})
})
