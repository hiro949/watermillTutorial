//go:generate mockgen -source=processor.go -destination=../mocks/processor_mock.go -package=mocks

// Package app はアプリケーション層の実装を提供します。
// Watermill を使ったメッセージ処理とドメインロジックを連携させます。
package app

import (
	"context"
	"fmt"
	"time"

	"watermillTutorial/domain"

	watermill "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// Application はアプリケーションサービス（DDD: Application Layer）を表します。
type Application struct {
	subscriber  message.Subscriber
	publisher   message.Publisher
	greeter     domain.Greeter
	inputTopic  string
	outputTopic string
	logger      watermill.LoggerAdapter
}

func NewApplication(
	subscriber message.Subscriber,
	publisher message.Publisher,
	greeter domain.Greeter,
	inTopic, outTopic string,
	logger watermill.LoggerAdapter,
) *Application {
	return &Application{
		subscriber:  subscriber,
		publisher:   publisher,
		greeter:     greeter,
		inputTopic:  inTopic,
		outputTopic: outTopic,
		logger:      logger,
	}
}

// Run は Watermill Router を使ってメッセージ処理を開始します。
func (a *Application) Run(ctx context.Context) error {
	router, err := message.NewRouter(message.RouterConfig{}, a.logger)
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	router.AddMiddleware(
		middleware.Recoverer,
		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: 100 * time.Millisecond,
			Logger:          a.logger,
		}.Middleware,
	)

	router.AddHandler(
		"greeting_handler",
		a.inputTopic,
		a.subscriber,
		a.outputTopic,
		a.publisher,
		a.handleMessage,
	)

	return router.Run(ctx)
}

// handleMessage は受信メッセージを処理し、出力メッセージを返すハンドラ関数です。
func (a *Application) handleMessage(msg *message.Message) ([]*message.Message, error) {
	t, err := parseTimePayload(msg.Payload)
	var out string
	if err != nil {
		a.logger.Error("parse time failed, publishing unknown", err, nil)
		out = "unknown"
	} else {
		out = a.greeter.Greet(t)
	}

	a.logger.Info("published message", map[string]any{"topic": a.outputTopic, "payload": out})
	outMsg := message.NewMessage(watermill.NewUUID(), []byte(out))
	return []*message.Message{outMsg}, nil
}
