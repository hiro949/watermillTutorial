package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	watermill "github.com/ThreeDotsLabs/watermill"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg := loadConfig()
	logger := watermill.NewStdLogger(false, false)

	subscriber, publisher, err := setupInfrastructure(cfg, logger)
	if err != nil {
		logger.Error("failed to setup infrastructure", err, nil)
		return err
	}

	greeter, err := setupDomain(cfg)
	if err != nil {
		logger.Error("failed to setup domain", err, nil)
		return err
	}

	application := setupApplication(cfg, subscriber, publisher, greeter, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("Starting application...", nil)
	if err := application.Run(ctx); err != nil {
		logger.Error("application error", err, nil)
		return err
	}
	logger.Info("Application stopped", nil)
	return nil
}
