package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	watermill "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"watermillTutorial/app"
	"watermillTutorial/domain"
	"watermillTutorial/infra/kafka"
)

// setupInfrastructure はインフラ層のコンポーネントを生成する
func setupInfrastructure(cfg *Config, logger watermill.LoggerAdapter) (app.SubscriberFactory, app.PublisherFactory, error) {
	broker := kafka.NewKafkaBroker(cfg.Brokers, cfg.ConsumerGroup, logger)
	subFactory, pubFactory := createFactories(broker)
	return subFactory, pubFactory, nil
}

// createFactories はBrokerからファクトリー関数を生成する
func createFactories(broker *kafka.KafkaBroker) (app.SubscriberFactory, app.PublisherFactory) {
	subscriberFactory := func() (message.Subscriber, error) {
		return broker.NewSubscriber()
	}

	publisherFactory := func() (message.Publisher, error) {
		return broker.NewPublisher()
	}

	return subscriberFactory, publisherFactory
}

// setupDomain はドメイン層のコンポーネントを生成する
func setupDomain(cfg *Config) (domain.Greeter, error) {
	return domain.NewGreeter(cfg.Timezone)
}

// setupApplication はアプリケーション層のコンポーネントを生成する
func setupApplication(
	cfg *Config,
	subFactory app.SubscriberFactory,
	pubFactory app.PublisherFactory,
	greeter domain.Greeter,
	logger watermill.LoggerAdapter,
) *app.Application {
	return app.NewApplication(
		subFactory,
		pubFactory,
		greeter,
		cfg.InputTopic,
		cfg.OutputTopic,
		logger,
	)
}

// setupGracefulShutdown はgracefulシャットダウンの仕組みを設定する
func setupGracefulShutdown(timeout time.Duration, logger watermill.LoggerAdapter) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go handleShutdownSignal(cancel, timeout, logger)
	return ctx, cancel
}

// handleShutdownSignal はシャットダウンシグナルを処理する
func handleShutdownSignal(cancelFunc context.CancelFunc, timeout time.Duration, logger watermill.LoggerAdapter) {
	sigCh := make(chan os.Signal, 1) // バッファサイズ1でシグナル取りこぼしを防ぐ
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh) // リソースクリーンアップ

	// シグナル待機
	<-sigCh
	logger.Info("Shutdown signal received, stopping gracefully...", nil)

	// アプリケーションのキャンセルをトリガー
	cancelFunc()

	// タイムアウト監視を別goroutineで実行
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
	defer shutdownCancel()

	go func() {
		<-shutdownCtx.Done()
		if shutdownCtx.Err() == context.DeadlineExceeded {
			logger.Error("Shutdown timeout exceeded, forcing exit", nil, nil)
			os.Exit(1)
		}
	}()
}
