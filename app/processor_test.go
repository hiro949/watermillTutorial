package app

import (
	"context"
	"testing"
	"time"

	"watermillTutorial/mocks"

	watermill "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/golang/mock/gomock"
)

func TestApplication_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockGreeter := mocks.NewMockGreeter(ctrl)

	logger := watermill.NewStdLogger(false, false)

	// メッセージチャネルを用意
	msgChan := make(chan *message.Message, 1)

	// Close メソッドのモック設定
	mockSub.EXPECT().Close().Return(nil).AnyTimes()
	mockPub.EXPECT().Close().Return(nil).AnyTimes()

	// Subscribe が msgChan を返すように設定
	mockSub.
		EXPECT().
		Subscribe(gomock.Any(), "input").
		Return(msgChan, nil)

	// Greeter の期待値
	mockGreeter.
		EXPECT().
		Greet(gomock.Any()).
		Return("hello")

	// Publisher の期待値
	mockPub.
		EXPECT().
		Publish("output", gomock.Any()).
		Return(nil)

	// ファクトリー関数を定義
	subscriberFactory := func() (message.Subscriber, error) {
		return mockSub, nil
	}
	publisherFactory := func() (message.Publisher, error) {
		return mockPub, nil
	}

	app := NewApplication(subscriberFactory, publisherFactory, mockGreeter, "input", "output", logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run を別 goroutine で実行
	go func() {
		_ = app.Run(ctx)
	}()

	// テスト用メッセージを送信
	payload := []byte(`{"time":"2024-01-01T00:00:00Z"}`)
	msg := message.NewMessage("id-1", payload)
	msgChan <- msg

	// 少し待つ
	time.Sleep(100 * time.Millisecond)
}
