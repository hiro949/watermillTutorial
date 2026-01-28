package main

import (
	"os"

	watermill "github.com/ThreeDotsLabs/watermill"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	// 設定の読み込み
	cfg := loadConfig()

	// ロガーの初期化
	logger := watermill.NewStdLogger(false, false)

	// インフラ層のセットアップ
	subscriberFactory, publisherFactory := setupInfrastructure(cfg, logger)

	// ドメイン層のセットアップ
	greeter, err := setupDomain(cfg)
	if err != nil {
		logger.Error("failed to setup domain", err, nil)
		return err
	}

	// アプリケーション層のセットアップ
	application := setupApplication(cfg, subscriberFactory, publisherFactory, greeter, logger)

	// Graceful shutdown のセットアップ
	ctx, cancel := setupGracefulShutdown(cfg.ShutdownTimeout, logger)
	defer cancel()

	// アプリケーションの実行
	logger.Info("Starting application...", nil)
	if err := application.Run(ctx); err != nil {
		logger.Error("application error", err, nil)
		return err
	}
	logger.Info("Application stopped", nil)
	return nil
}
