package app

import (
	"testing"

	"watermillTutorial/mocks"

	watermill "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/golang/mock/gomock"
)

func TestHandleMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGreeter := mocks.NewMockGreeter(ctrl)
	logger := watermill.NewStdLogger(false, false)

	mockGreeter.
		EXPECT().
		Greet(gomock.Any()).
		Return("hello")

	a := NewApplication(nil, nil, mockGreeter, "input", "output", logger)

	payload := []byte(`{"time":"2024-01-01T00:00:00Z"}`)
	msg := message.NewMessage("id-1", payload)

	out, err := a.handleMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(out))
	}
	if string(out[0].Payload) != "hello" {
		t.Errorf("expected payload 'hello', got '%s'", string(out[0].Payload))
	}
}

func TestHandleMessage_InvalidPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGreeter := mocks.NewMockGreeter(ctrl)
	logger := watermill.NewStdLogger(false, false)

	a := NewApplication(nil, nil, mockGreeter, "input", "output", logger)

	msg := message.NewMessage("id-2", []byte(`invalid`))

	out, err := a.handleMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(out))
	}
	if string(out[0].Payload) != "unknown" {
		t.Errorf("expected payload 'unknown', got '%s'", string(out[0].Payload))
	}
}
