package redis_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/timehop/jimmy/redis"
)

var _ = Describe("Pool", func() {
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
})
