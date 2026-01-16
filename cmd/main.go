package main

import (
	"log"

	watermill "github.com/ThreeDotsLabs/watermill"
)

func main() {
	// 設定の読み込み
	cfg := loadConfig()

	// ロガーの初期化
	logger := watermill.NewStdLogger(false, false)

	// インフラ層のセットアップ
	subscriberFactory, publisherFactory, err := setupInfrastructure(cfg, logger)
	if err != nil {
		logger.Error("failed to setup infrastructure", err, nil)
		log.Fatalf("failed to setup infrastructure: %v", err)
	}

	// ドメイン層のセットアップ
	greeter, err := setupDomain(cfg)
	if err != nil {
		logger.Error("failed to setup domain", err, nil)
		log.Fatalf("failed to setup domain: %v", err)
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
		log.Fatalf("application error: %v", err)
	}
	logger.Info("Application stopped", nil)
}
