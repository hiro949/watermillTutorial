package kafka

import (
	watermill "github.com/ThreeDotsLabs/watermill"
	"testing"
)

func TestNewKafkaBroker(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)
	broker := NewKafkaBroker([]string{"localhost:9092"}, "test-group", logger)
	if broker == nil {
		t.Fatal("expected broker instance, got nil")
	}
}
