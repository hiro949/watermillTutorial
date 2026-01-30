package main

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"watermillTutorial/app"
	"watermillTutorial/domain"
	"watermillTutorial/infra/kafka"
)

// setupInfrastructure はインフラ層のコンポーネントを生成する
func setupInfrastructure(cfg *Config, logger watermill.LoggerAdapter) (message.Subscriber, message.Publisher, error) {
	broker := kafka.NewBroker(cfg.Brokers, cfg.ConsumerGroup, logger)

	subscriber, err := broker.NewSubscriber()
	if err != nil {
		return nil, nil, err
	}

	publisher, err := broker.NewPublisher()
	if err != nil {
		return nil, nil, err
	}

	return subscriber, publisher, nil
}

// setupDomain はドメイン層のコンポーネントを生成する
func setupDomain(cfg *Config) (domain.Greeter, error) {
	return domain.NewGreeter(cfg.Timezone)
}

// setupApplication はアプリケーション層のコンポーネントを生成する
func setupApplication(
	cfg *Config,
	subscriber message.Subscriber,
	publisher message.Publisher,
	greeter domain.Greeter,
	logger watermill.LoggerAdapter,
) *app.Application {
	return app.NewApplication(
		subscriber,
		publisher,
		greeter,
		cfg.InputTopic,
		cfg.OutputTopic,
		logger,
	)
}
