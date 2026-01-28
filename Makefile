.PHONY: help build start stop restart logs logs-app logs-kafka status ps clean test-producer test-consumer integration-test up down rebuild reset test lint lint-fix

# デフォルトターゲット
.DEFAULT_GOAL := help

# スクリプトディレクトリ
SCRIPTS_DIR := scripts

help: ## ヘルプを表示
	@bash $(SCRIPTS_DIR)/help.sh

build: ## Dockerイメージをビルド
	@bash $(SCRIPTS_DIR)/build.sh

start: ## サービスを起動
	@bash $(SCRIPTS_DIR)/start.sh

stop: ## サービスを停止
	@bash $(SCRIPTS_DIR)/stop.sh

restart: ## サービスを再起動
	@$(MAKE) --no-print-directory stop
	@sleep 2
	@$(MAKE) --no-print-directory start

logs: ## すべてのログを表示
	@docker-compose logs -f

logs-app: ## アプリケーションのログを表示
	@docker-compose logs -f app

logs-kafka: ## Kafkaのログを表示
	@docker-compose logs -f kafka

status: ## サービスのステータスを表示
	@docker-compose ps

ps: status ## サービスのステータスを表示（エイリアス）

clean: ## すべてのコンテナとボリュームを削除
	@bash $(SCRIPTS_DIR)/clean.sh

test-producer: ## Kafkaプロデューサーのテスト（メッセージ送信）
	@bash $(SCRIPTS_DIR)/test-producer.sh

test-consumer: ## Kafkaコンシューマーのテスト（メッセージ受信）
	@bash $(SCRIPTS_DIR)/test-consumer.sh

integration-test: ## 統合テストを実行（自動検証）
	@bash $(SCRIPTS_DIR)/integration-test.sh

up: start ## サービスを起動（エイリアス）

down: stop ## サービスを停止（エイリアス）

rebuild: ## イメージを再ビルドして起動
	@$(MAKE) --no-print-directory build
	@$(MAKE) --no-print-directory start

reset: ## 完全にクリーンアップして再構築
	@$(MAKE) --no-print-directory clean
	@$(MAKE) --no-print-directory build
	@$(MAKE) --no-print-directory start

# Go開発ツール
test: ## Goテストを実行
	@go test -v ./...

lint: ## コード品質チェック（golangci-lint）
	@golangci-lint run ./...

lint-fix: ## コード品質チェック＋自動修正
	@golangci-lint run --fix ./...
