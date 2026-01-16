//go:generate mockgen -source=processor.go -destination=../mocks/processor_mock.go -package=mocks

// Package app はアプリケーション層の実装を提供します。
// Watermill を使ったメッセージ処理とドメインロジックを連携させます。
package app

import (
	"context"
	"fmt"

	"watermillTutorial/domain"

	watermill "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// SubscriberFactory は Subscriber を作成する関数型
type SubscriberFactory func() (message.Subscriber, error)

// PublisherFactory は Publisher を作成する関数型
type PublisherFactory func() (message.Publisher, error)

// Application はアプリケーションサービス（DDD: Application Layer）を表します。
// domain の Greeter を使ってビジネスルールを適用し、インフラ（publisher/subscriber）と連携します。
type Application struct {
	subscriberFactory SubscriberFactory
	publisherFactory  PublisherFactory
	greeter           domain.Greeter
	inputTopic        string
	outputTopic       string
	logger            watermill.LoggerAdapter
}

func NewApplication(subFactory SubscriberFactory, pubFactory PublisherFactory, greeter domain.Greeter, inTopic, outTopic string, logger watermill.LoggerAdapter) *Application {
	return &Application{
		subscriberFactory: subFactory,
		publisherFactory:  pubFactory,
		greeter:           greeter,
		inputTopic:        inTopic,
		outputTopic:       outTopic,
		logger:            logger,
	}
}

// Run はメッセージ受信ループを開始します。
func (a *Application) Run(ctx context.Context) error {
	// ファクトリー関数から subscriber を作成
	subscriber, err := a.subscriberFactory()
	if err != nil {
		return fmt.Errorf("failed to create subscriber: %w", err)
	}
	defer func() { _ = subscriber.Close() }()

	// ファクトリー関数から publisher を作成
	publisher, err := a.publisherFactory()
	if err != nil {
		return fmt.Errorf("failed to create publisher: %w", err)
	}
	defer func() { _ = publisher.Close() }()

	msgs, err := subscriber.Subscribe(ctx, a.inputTopic)
	if err != nil {
		return fmt.Errorf("subscribe error: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("application shutting down", nil)
			return nil
		case msg, ok := <-msgs:
			if !ok {
				a.logger.Info("message channel closed", nil)
				return nil
			}

			t, err := parseTimePayload(msg.Payload)
			var out string
			if err != nil {
				a.logger.Error("parse time failed, publishing unknown", err, nil)
				out = "unknown"
			} else {
				out = a.greeter.Greet(t)
			}

			publishMsg := message.NewMessage(watermill.NewUUID(), []byte(out))
			if err := publisher.Publish(a.outputTopic, publishMsg); err != nil {
				a.logger.Error("publish error", err, nil)
			} else {
				a.logger.Info("published message", map[string]interface{}{"topic": a.outputTopic, "payload": out})
			}

			msg.Ack()
		}
	}
}
