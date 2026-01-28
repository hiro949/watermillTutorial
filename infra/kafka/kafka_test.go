package kafka

import (
	"testing"

	watermill "github.com/ThreeDotsLabs/watermill"
)

func TestNewBroker(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)
	broker := NewBroker([]string{"localhost:9092"}, "test-group", logger)
	if broker == nil {
		t.Fatal("expected broker instance, got nil")
	}
}
