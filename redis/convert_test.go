package redis

import (
	"errors"
	"testing"
)

func TestStringMap(t *testing.T) {
	t.Run("nil slice and nil error returns error", func(t *testing.T) {
		result, err := stringMap(nil, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("nil slice and non-nil error returns that error", func(t *testing.T) {
		inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
		result, err := stringMap(nil, inputErr)
		if err != inputErr {
			t.Errorf("got %v, want %v", err, inputErr)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("odd-length slice and nil error returns error", func(t *testing.T) {
		result, err := stringMap([]string{"foo"}, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("odd-length slice and non-nil error returns that error", func(t *testing.T) {
		inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
		result, err := stringMap([]string{"foo"}, inputErr)
		if err != inputErr {
			t.Errorf("got %v, want %v", err, inputErr)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})
}

func TestSpliceMap(t *testing.T) {
	t.Run("all nil args returns error", func(t *testing.T) {
		result, err := spliceMap(nil, nil, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("nil keys nil vals and non-nil error returns that error", func(t *testing.T) {
		inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
		result, err := spliceMap(nil, nil, inputErr)
		if err != inputErr {
			t.Errorf("got %v, want %v", err, inputErr)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("2 keys 1 val and nil error returns error", func(t *testing.T) {
		result, err := spliceMap([]string{"foo", "bar"}, []string{"baz"}, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("1 key 2 vals and non-nil error returns that error", func(t *testing.T) {
		inputErr := errors.New("The cheese is old and moldy, where is the bathroom?")
		result, err := spliceMap([]string{"foo"}, []string{"foo", "bar"}, inputErr)
		if err != inputErr {
			t.Errorf("got %v, want %v", err, inputErr)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})
}
