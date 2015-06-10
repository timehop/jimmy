package redis

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TODO: this mostly tests error cases. Add tests for success cases!

var _ = Describe("stringMap", func() {
	Context("A nil slice arg and a nil error arg", func() {
		It("returns nil and an error", func() {
			result, err := stringMap(nil, nil)
			Expect(err).ToNot(BeNil())
			Expect(result).To(BeNil())
		})
	})

	Context("A nil slice arg and a non-nil error arg", func() {
		It("returns nil and the error that was passed in", func() {
			inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
			result, err := stringMap(nil, inputErr)
			Expect(err).To(Equal(inputErr))
			Expect(&err).To(Equal(&inputErr))
			Expect(result).To(BeNil())
		})
	})

	Context("An odd-length slice arg and a nil error arg", func() {
		It("returns nil and an error", func() {
			result, err := stringMap([]string{"foo"}, nil)
			Expect(err).ToNot(BeNil())
			Expect(result).To(BeNil())
		})
	})

	Context("An odd-length slice arg and a non-nil error arg", func() {
		It("returns nil and the error that was passed in", func() {
			inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
			result, err := stringMap([]string{"foo"}, inputErr)
			Expect(err).To(Equal(inputErr))
			Expect(&err).To(Equal(&inputErr))
			Expect(result).To(BeNil())
		})
	})
})

var _ = Describe("spliceMap", func() {
	Context("All nil args", func() {
		It("returns nil and an error", func() {
			result, err := spliceMap(nil, nil, nil)
			Expect(err).ToNot(BeNil())
			Expect(result).To(BeNil())
		})
	})

	Context("nil keys, nil vals, and a non-nil error", func() {
		It("returns nil and the error that was passed in", func() {
			inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
			result, err := spliceMap(nil, nil, inputErr)
			Expect(err).To(Equal(inputErr))
			Expect(&err).To(Equal(&inputErr))
			Expect(result).To(BeNil())
		})
	})

	Context("2 keys, 1 val, and a nil error", func() {
		It("returns nil and an error", func() {
			result, err := spliceMap([]string{"foo", "bar"}, []string{"baz"}, nil)
			Expect(err).ToNot(BeNil())
			Expect(result).To(BeNil())
		})
	})

	Context("1 key, 2 vals, and a non-nil error arg", func() {
		It("returns nil and the error that was passed in", func() {
			inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
			result, err := spliceMap([]string{"foo"}, []string{"foo", "bar"}, inputErr)
			Expect(err).To(Equal(inputErr))
			Expect(&err).To(Equal(&inputErr))
			Expect(result).To(BeNil())
		})
	})
})
