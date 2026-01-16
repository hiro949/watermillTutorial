package main

import (
	"os"
	"strings"
	"time"
)

// Config はアプリケーション設定を保持する構造体
type Config struct {
	Brokers         []string
	ConsumerGroup   string
	InputTopic      string
	OutputTopic     string
	Timezone        string
	ShutdownTimeout time.Duration
}

// loadConfig は環境変数から設定を読み込む
func loadConfig() *Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP")
	if consumerGroup == "" {
		consumerGroup = "watermill-group"
	}

	inputTopic := os.Getenv("INPUT_TOPIC")
	if inputTopic == "" {
		inputTopic = "good-morning-input"
	}

	outputTopic := os.Getenv("OUTPUT_TOPIC")
	if outputTopic == "" {
		outputTopic = "ohayou-output"
	}

	timezone := os.Getenv("TIMEZONE")
	if timezone == "" {
		timezone = "Asia/Tokyo"
	}

	shutdownTimeout := 30 * time.Second
	if timeout := os.Getenv("SHUTDOWN_TIMEOUT"); timeout != "" {
		if d, err := time.ParseDuration(timeout); err == nil {
			shutdownTimeout = d
		}
	}

	return &Config{
		Brokers:         strings.Split(brokers, ","),
		ConsumerGroup:   consumerGroup,
		InputTopic:      inputTopic,
		OutputTopic:     outputTopic,
		Timezone:        timezone,
		ShutdownTimeout: shutdownTimeout,
	}
}
