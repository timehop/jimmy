package redis_test

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/timehop/jimmy/redis"
)

var _ = Describe("Connection", func() {
	c, _ := redis.NewConnection("redis://localhost:6379")
	Describe("PFAdd", func() {
		It("Should indicate HyperLogLog register was altered (ie: 1)", func() {
			// Clean up the key
			c.Del("_tests:jimmy:redis:foo1")

			// Subject
			i, err := c.PFAdd("_tests:jimmy:redis:foo1", "bar")
			Expect(err).To(BeNil())
			Expect(i).To(Equal(1))
		})
		It("Should indicate HyperLogLog register was not altered (ie: 0)", func() {

			// Subject
			_, err := c.PFAdd("_tests:jimmy:redis:foo2", "bar")
			Expect(err).To(BeNil())
			i, err := c.PFAdd("_tests:jimmy:redis:foo2", "bar")
			Expect(err).To(BeNil())
			Expect(i).To(Equal(0))
		})
	})
	Describe("PFCount", func() {
		It("Should return the approximate cardinality of the HLL", func() {
			c.Del("_tests:jimmy:redis:foo3")
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
})
