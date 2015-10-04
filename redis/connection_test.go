package redis_test

import (
	"fmt"

	netURL "net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/timehop/jimmy/redis"
)

var _ = Describe("Connection", func() {

	// Using an arbitrary password should fallback to using no password
	url := "redis://:foopass@localhost:6379/10"
	parsedURL, _ := netURL.Parse(url)
	c, err := redis.NewConnection(parsedURL)
	if err != nil {
		panic(err)
	}

	BeforeEach(func() {
		c.Do("FLUSHDB")
	})

	Describe("NewConnection", func() {
		// Assumes redis' default state is auth-less

		Context("server has no auth set", func() {
			It("should ping without auth", func() {
				url := "redis://localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, err := redis.NewConnection(parsedURL)
				Expect(err).To(BeNil())

				_, err = c.Do("PING")
				Expect(err).To(BeNil())
			})
			It("should fallback to ping without auth", func() {
				url := "redis://user:testpass@localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, err := redis.NewConnection(parsedURL)
				Expect(err).To(BeNil())

				_, err = c.Do("PING")
				Expect(err).To(BeNil())
			})
		})

		Context("server requires auth", func() {
			BeforeEach(func() {
				url := "redis://localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)
				c.Do("CONFIG", "SET", "requirepass", "testpass")
			})
			AfterEach(func() {
				url := "redis://:testpass@localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)
				c.Do("CONFIG", "SET", "requirepass", "")
			})

			It("should fail to ping without auth", func() {
				url := "redis://localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)

				_, err := c.Do("PING")
				Expect(err).ToNot(BeNil())
			})
			It("should successfully ping with auth", func() {
				url := "redis://:testpass@localhost:6379"
				parsedURL, _ := netURL.Parse(url)
				c, _ := redis.NewConnection(parsedURL)

				_, err := c.Do("PING")
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("TTL", func() {
		Context("Without a key.", func() {
			It("Should return -2.", func() {
				i, err := c.TTL("foo")
				Expect(err).To(BeNil())
				Expect(i).To(Equal(-2))
			})
		})
		Context("With a key without an expiration.", func() {
			It("Should return -1.", func() {
				c.Set("foo", "bar")

				i, err := c.TTL("foo")
				Expect(err).To(BeNil())
				Expect(i).To(Equal(-1))
			})
		})
		Context("With a key with an expiration.", func() {
			It("Should return the time to live.", func() {
				c.SetEx("biz", "baz", 15)

				i, err := c.TTL("biz")
				Expect(err).To(BeNil())
				Expect(i).To(Equal(15))
			})
		})
	})

	Describe("PFAdd", func() {
		It("Should indicate HyperLogLog register was altered (ie: 1)", func() {
			i, err := c.PFAdd("_tests:jimmy:redis:foo1", "bar")
			Expect(err).To(BeNil())
			Expect(i).To(Equal(1))
		})
		It("Should indicate HyperLogLog register was not altered (ie: 0)", func() {
			_, err := c.PFAdd("_tests:jimmy:redis:foo2", "bar")
			Expect(err).To(BeNil())
			i, err := c.PFAdd("_tests:jimmy:redis:foo2", "bar")
			Expect(err).To(BeNil())
			Expect(i).To(Equal(0))
		})
	})

	Describe("PFCount", func() {
		It("Should return the approximate cardinality of the HLL", func() {
			var actualCardinality float64 = 20000
			for i := 0; float64(i) < actualCardinality; i++ {
				_, err := c.PFAdd("_tests:jimmy:redis:foo3", fmt.Sprint(i))
				Expect(err).To(BeNil())
			}
			card, err := c.PFCount("_tests:jimmy:redis:foo3")
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
				_, err := c.PFAdd("_tests:jimmy:redis:hll1", fmt.Sprint(x))
				Expect(err).To(BeNil())
			}

			for _, x := range setB {
				_, err := c.PFAdd("_tests:jimmy:redis:hll2", fmt.Sprint(x))
				Expect(err).To(BeNil())
			}

			for _, x := range setC {
				_, err := c.PFAdd("_tests:jimmy:redis:hll3", fmt.Sprint(x))
				Expect(err).To(BeNil())
			}

			for i := 1; i < 4; i++ {
				card, err := c.PFCount(fmt.Sprintf("_tests:jimmy:redis:hll%d", i))
				Expect(err).To(BeNil())
				Expect(card).To(Equal(5))
			}

			ok, err := c.PFMerge("_tests:jimmy:redis:hll1+2", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())

			card, err := c.PFCount("_tests:jimmy:redis:hll1+2")
			Expect(err).To(BeNil())
			Expect(card).To(Equal(7))

			ok, err = c.PFMerge("_tests:jimmy:redis:hll1+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll3")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())

			card, err = c.PFCount("_tests:jimmy:redis:hll1+3")
			Expect(err).To(BeNil())
			Expect(card).To(Equal(10))

			ok, err = c.PFMerge("_tests:jimmy:redis:hll1+2+3", "_tests:jimmy:redis:hll1", "_tests:jimmy:redis:hll2", "_tests:jimmy:redis:hll3")
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())

			card, err = c.PFCount("_tests:jimmy:redis:hll1+2+3")
			Expect(err).To(BeNil())
			Expect(card).To(Equal(12))
		})
	})

	Describe("LTrim", func() {
		Context("When a list is trimmed", func() {
			It("Trims the list", func() {
				key := "_tests:jimmy:redis:list"

				for i := 0; i < 5; i++ {
					c.LPush(key, fmt.Sprint(i))
				}

				size, err := c.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(5))

				// Trim nothing
				err = c.LTrim(key, 0, 4)
				Expect(err).To(BeNil())

				size, err = c.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(5))

				// Trim first element
				err = c.LTrim(key, 1, 5)
				Expect(err).To(BeNil())

				size, err = c.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(4))

				item, err := c.LPop(key)
				Expect(err).To(BeNil())
				Expect(item).To(Equal("3"))

				// Trim last element
				err = c.LTrim(key, -4, -1)
				Expect(err).To(BeNil())

				size, err = c.LLen(key)
				Expect(err).To(BeNil())
				Expect(size).To(Equal(3))

				item, err = c.LPop(key)
				Expect(err).To(BeNil())
				Expect(item).To(Equal("2"))
			})
		})

		Context("When a not-list is trimmed", func() {
			It("Returns an error", func() {
				key := "_tests:jimmy:redis:not-list"

				Expect(c.Set(key, "yay")).To(BeNil())
				Expect(c.LTrim(key, 0, 4)).ToNot(BeNil())

				c.Del(key)
				_, err := c.SAdd(key, "yay")
				Expect(err).To(BeNil())
				Expect(c.LTrim(key, 0, 4)).ToNot(BeNil())
			})
		})
	})

	Describe("LRange", func() {
		Context("When an empty list is ranged", func() {
			It("Returns nothing, but no err", func() {
				key := "_tests:jimmy:redis:list"
				things, err := c.LRange(key, 0, -1)
				Expect(err).To(BeNil())
				Expect(things).To(BeEmpty())
			})
		})

		Context("When a list is ranged", func() {
			It("Returns the items", func() {
				key := "_tests:jimmy:redis:list"
				for i := 0; i < 5; i++ {
					_, err := c.LPush(key, fmt.Sprint(i))
					Expect(err).To(BeNil())
				}

				things, err := c.LRange(key, 0, -1)
				Expect(err).To(BeNil())
				Expect(len(things)).To(Equal(5))

				things, err = c.LRange(key, 0, 0)
				Expect(err).To(BeNil())
				Expect(len(things)).To(Equal(1))
				Expect(things[0]).To(Equal("4"))

				things, err = c.LRange(key, -1, -1)
				Expect(err).To(BeNil())
				Expect(len(things)).To(Equal(1))
				Expect(things[0]).To(Equal("0"))
			})
		})
	})

	Describe("SMove", func() {
		It("Should move the member to the other set", func() {
			key := "_tests:jimmy:redis:smove"

			c.SAdd(key+":a", "foobar")

			moved, err := c.SMove(key+":a", key+":b", "foobar")
			Expect(err).To(BeNil())
			Expect(moved).To(BeTrue())

			members, _ := c.SMembers(key + ":a")
			Expect(len(members)).To(Equal(0))

			members, _ = c.SMembers(key + ":b")
			Expect(members).To(Equal([]string{"foobar"}))
		})
	})

	Describe("ZScan", func() {
		It("Should scan the sorted set", func() {
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
			Expect(err).To(BeNil())
			scanned = append(scanned, matches...)
			scannedScores = append(scannedScores, scores...)
			for cursor != 0 {
				cursor, matches, scores, err = c.ZScan(key, cursor, "", 1)
				Expect(err).To(BeNil())
				scannedScores = append(scannedScores, scores...)
			}

			Expect(len(scanned)).To(Equal(5))
			Expect(scanned).To(ContainElement("a"))
			Expect(scanned).To(ContainElement("b"))
			Expect(scanned).To(ContainElement("c"))
			Expect(scanned).To(ContainElement("d"))
			Expect(scanned).To(ContainElement("e"))

			for i, elem := range scanned {
				switch elem {
				case "a":
					Expect(scannedScores[i]).To(Equal(float64(1)))
				case "b":
					Expect(scannedScores[i]).To(Equal(float64(2)))
				case "c":
					Expect(scannedScores[i]).To(Equal(float64(3)))
				case "d":
					Expect(scannedScores[i]).To(Equal(float64(4)))
				case "e":
					Expect(scannedScores[i]).To(Equal(float64(5)))
				}
			}

		})
	})

	Describe("SScan", func() {
		It("Should scan the set", func() {
			key := "_tests:jimmy:redis:sscan"

			c.SAdd(key, "a", "b", "c", "d", "e")

			var scanned []string
			var cursor int
			var matches []string
			var err error

			cursor, matches, err = c.SScan(key, cursor, "", 1)
			Expect(err).To(BeNil())
			scanned = append(scanned, matches...)
			for cursor != 0 {
				cursor, matches, err = c.SScan(key, cursor, "", 1)
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
				mustSucceed2(c.HSet("foo", "bar", "baz"))
				val, err := c.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("baz"))
			})
		})

		Context("a key that exists and contains a hash that doesn’t contain the requested field", func() {
			It("returns an error and an empty string", func() {
				mustSucceed2(c.HSet("foo", "blah", "blech"))
				val, err := c.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))
			})
		})

		Context("a key that doesn’t exist", func() {
			It("returns an error and an empty string", func() {
				val, err := c.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))
			})
		})

		Context("a key that exists but doesn’t contain a hash", func() {
			It("returns an error and an empty string", func() {
				mustSucceed1(c.Set("foo", "yo"))
				val, err := c.HGet("foo", "bar")
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
				err := c.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := c.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
			})
		})

		Context("a key that doesn’t exist", func() {
			It("returns an empty map and no error", func() {
				vals, err := c.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(0))
			})
		})
	})

	Describe("HSet", func() {
		Context("a key that doesn’t already exist and two strings", func() {
			It("returns true and nil and contain the new pair", func() {
				isNew, err := c.HSet("foo", "bar", "baz")
				Expect(err).To(BeNil())
				Expect(isNew).To(Equal(true))

				val, err := c.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("baz"))
			})
		})

		Context("a key that already exists and a field that it doesn’t already contain", func() {
			It("returns true and nil and contain both fields", func() {
				mustSucceed2(c.HSet("foo", "bar", "baz"))
				isNew, err := c.HSet("foo", "yo", "oy")
				Expect(err).To(BeNil())
				Expect(isNew).To(Equal(true))

				val, err := c.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("baz"))

				val, err = c.HGet("foo", "yo")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("oy"))
			})
		})

		Context("a key that already exists and a field that it already contains", func() {
			It("returns false and nil and change the value of the field", func() {
				mustSucceed2(c.HSet("foo", "bar", "baz"))
				isNew, err := c.HSet("foo", "bar", "yo")
				Expect(err).To(BeNil())
				Expect(isNew).To(Equal(false))

				val, err := c.HGet("foo", "bar")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("yo"))
			})
		})

		Context("a key that already exists and is not a hash", func() {
			It("returns false and an error", func() {
				mustSucceed1(c.Set("foo", "bar"))
				isNew, err := c.HSet("foo", "bar", "yo")
				Expect(err).ToNot(BeNil())
				Expect(isNew).To(Equal(false))

				val, err := c.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))

				val, err = c.Get("foo")
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
				err := c.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := c.HMGet("foo", "bar", "blah")
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
				err := c.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := c.HMGet("foo", "bar", "yo", "blah")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(3))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("yo", ""))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
			})
		})

		Context("a key that doesn’t exist", func() {
			It("returns nil values and no error", func() {
				vals, err := c.HMGet("foo", "bar", "blah")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(2))
				Expect(vals).To(HaveKeyWithValue("bar", ""))
				Expect(vals).To(HaveKeyWithValue("blah", ""))
			})
		})

		Context("no fields", func() {
			It("returns nil values and an error", func() {
				vals, err := c.HMGet("foo")
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
				err := c.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := c.HGetAll("foo")
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
				err := c.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := c.HGetAll("foo")
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
				err := c.HMSet("foo", in)
				Expect(err).To(BeNil())

				in = map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err = c.HMSet("foo", in)
				Expect(err).To(BeNil())

				vals, err := c.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(3))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
				Expect(vals).To(HaveKeyWithValue("blah", "blech"))
				Expect(vals).To(HaveKeyWithValue("yo", "oy"))
			})
		})

		Context("a key that already exists and is not a hash", func() {
			It("returns false and an error", func() {
				mustSucceed1(c.Set("foo", "bar"))

				in := map[string]interface{}{
					"bar":  "baz",
					"blah": "blech",
				}
				err := c.HMSet("foo", in)
				Expect(err).ToNot(BeNil())

				val, err := c.HGet("foo", "bar")
				Expect(err).ToNot(BeNil())
				Expect(val).To(Equal(""))

				val, err = c.Get("foo")
				Expect(err).To(BeNil())
				Expect(val).To(Equal("bar"))
			})
		})

		Context("a key that already exists and an empty map", func() {
			It("returns an error and doesn’t change existing the key", func() {
				mustSucceed2(c.HSet("foo", "bar", "baz"))
				in := map[string]interface{}{}
				err := c.HMSet("foo", in)
				Expect(err).ToNot(BeNil())

				vals, err := c.HGetAll("foo")
				Expect(err).To(BeNil())
				Expect(vals).To(HaveLen(1))
				Expect(vals).To(HaveKeyWithValue("bar", "baz"))
			})
		})

		Context("a key that doesn’t already exist and an empty map", func() {
			It("returns an error and doesn’t create the key", func() {
				in := map[string]interface{}{}
				err := c.HMSet("foo", in)
				Expect(err).ToNot(BeNil())

				exists, err := c.Exists("foo")
				Expect(err).To(BeNil())
				Expect(exists).To(BeFalse())
			})
		})
	})
})

func mustSucceed1(err error) {
	if err != nil {
		Fail("Expected " + err.Error() + " to be nil")
	}
}

func mustSucceed2(_ interface{}, err error) {
	if err != nil {
		Fail("Expected " + err.Error() + " to be nil")
	}
}
