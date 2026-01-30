# アーキテクチャ設計

## レイヤー構造

本プロジェクトはクリーンアーキテクチャに基づいた3層構造を採用しています。

```text
┌─────────────────────────────────────────────────────┐
│                  cmd (main, config, setup)          │  ← Composition Root
│                 エントリーポイント層                   │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│                  app (application)                  │  ← アプリケーション層
│              ユースケースのオーケストレーション           │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│                    domain                           │  ← ドメイン層
│              ビジネスロジックの中核                     │
└─────────────────────────────────────────────────────┘

         ↑                              ↑
         │ (依存の逆転)                  │
         │                              │
┌─────────────────────────────────────────────────────┐
│              infra/kafka                            │  ← インフラ層
│           外部システムとの連携                          │
└─────────────────────────────────────────────────────┘
```

## ディレクトリ構造

```text
.
├── cmd/                    # エントリーポイント層
│   ├── main.go            # アプリケーションのエントリーポイント
│   ├── config.go          # 設定管理
│   └── setup.go           # 依存関係の組み立て
│
├── domain/                 # ドメイン層（最も内側）
│   ├── doc.go             # パッケージドキュメント
│   ├── greeting.go        # ドメインサービス
│   └── greeting_test.go   # ドメイン層のテスト
│
├── app/                    # アプリケーション層
│   ├── doc.go             # パッケージドキュメント
│   ├── processor.go       # メッセージ処理ロジック（Router + Handler）
│   ├── processor_test.go  # アプリケーション層のテスト
│   └── time_parser.go     # ユーティリティ
│
├── infra/                  # インフラ層（最も外側）
│   └── kafka/
│       ├── doc.go         # パッケージドキュメント
│       ├── kafka.go       # Kafka実装
│       └── kafka_test.go  # インフラ層のテスト
│
├── mocks/                  # テスト用モック
│   ├── doc.go             # パッケージドキュメント
│   ├── greeting_mock.go   # domain.Greeterのモック
│   ├── kafka_mock.go      # BrokerComponentのモック
│   ├── processor_mock.go  # Processorのモック
│   └── watermill_mock.go  # Watermill interfaceのモック
│
├── README.md               # プロジェクト概要
├── ARCHITECTURE.md         # アーキテクチャドキュメント（本ファイル）
├── go.mod
└── go.sum
```

## 各レイヤーの責務

### 1. Domain層 (`domain/`)

**責務:**

- ビジネスロジックの実装
- ドメインモデルの定義
- ビジネスルールの集約

**特徴:**

- 他のレイヤーに依存しない
- 標準ライブラリのみを使用
- 最も変更されにくい安定した層

**含まれるもの:**

- `Greeter`: 時間帯に応じた挨拶メッセージを生成するドメインサービス

### 2. Application層 (`app/`)

**責務:**

- ユースケースの実装
- ドメイン層とインフラ層の調整
- Watermill Router によるメッセージ処理パイプラインの構築

**特徴:**

- Domain層に依存
- Watermillインターフェースに依存（抽象に依存）
- 具体的なインフラ実装には依存しない（DIパターン）

**含まれるもの:**

- `Application`: Watermill Router ベースのメッセージ処理アプリケーション
- `handleMessage`: 入力メッセージを処理し出力メッセージを返すハンドラ関数
- ミドルウェア: `Recoverer`（パニック回復）、`Retry`（再試行）

### 3. Infrastructure層 (`infra/`)

**責務:**

- 外部システムとの連携
- フレームワーク/ライブラリの具体実装
- データの永続化・取得

**特徴:**

- Domain層やApplication層には依存しない
- Watermillインターフェースの具体実装を提供
- 最も変更されやすい層

**含まれるもの:**

- `KafkaBroker`: Kafka SubscriberとPublisherの生成
- `BrokerComponent`: Broker生成インターフェース

### 4. Entry Point層 (`cmd/`)

**責務:**

- アプリケーションの起動
- 設定の読み込み
- 依存関係の組み立て（Composition Root）

**特徴:**

- すべての層に依存
- 具体的な実装を組み立ててアプリケーションを構成
- Subscriber/Publisherを直接生成してApplicationに注入

**含まれるもの:**

- `main.go`: エントリーポイント（`signal.NotifyContext`によるシグナルハンドリング）
- `config.go`: 環境変数からの設定読み込み
- `setup.go`: 各レイヤーのセットアップ関数

## 依存関係のルール

### 依存の方向

```text
cmd → app → domain
      ↑
      │ (依存の逆転)
      │
    infra
```

1. **内側への依存のみ許可**: 外側の層は内側の層に依存できますが、内側の層は外側の層に依存できません
2. **抽象への依存**: Application層はInfrastructure層の具体実装ではなく、Watermillインターフェース（`message.Subscriber` / `message.Publisher`）に依存します
3. **依存性の注入**: 具体的な実装はComposition Root（cmd層）で生成し、直接注入されます

### 依存性逆転の原則（DIP）の実現

```go
// app層: Watermillインターフェースに依存
type Application struct {
    subscriber  message.Subscriber
    publisher   message.Publisher
    greeter     domain.Greeter
    // ...
}

// cmd層: 具体実装を生成して注入
subscriber, publisher, err := setupInfrastructure(cfg, logger)
application := app.NewApplication(subscriber, publisher, greeter, ...)
```

Application層は`message.Subscriber` / `message.Publisher`というWatermillインターフェースに依存し、Kafka実装の詳細を知りません。

## テスト戦略

### 単体テスト

各レイヤーは独立してテスト可能です：

- **Domain層**: 純粋関数のテストが中心
- **Application層**: ハンドラ関数（`handleMessage`）を直接テスト。Subscriber/Publisherはnilで済むため、Greeterのモックのみ必要
- **Infrastructure層**: 実際の依存を使用した統合テスト

### モックの使用

`mocks/`ディレクトリには、gomockで生成されたモックが配置されています：

```go
// テスト例: ハンドラ関数を直接テスト
mockGreeter := mocks.NewMockGreeter(ctrl)
mockGreeter.EXPECT().Greet(gomock.Any()).Return("hello")

a := NewApplication(nil, nil, mockGreeter, "input", "output", logger)
out, err := a.handleMessage(msg)
```

## デザインパターン

### 1. Router Pattern

Watermill Router を使用して、メッセージ処理パイプラインを宣言的に定義：

```go
router, _ := message.NewRouter(message.RouterConfig{}, a.logger)

router.AddMiddleware(
    middleware.Recoverer,
    middleware.Retry{MaxRetries: 3, ...}.Middleware,
)

router.AddHandler(
    "greeting_handler",
    a.inputTopic, a.subscriber,
    a.outputTopic, a.publisher,
    a.handleMessage,
)

router.Run(ctx)
```

### 2. Middleware Pattern

横断的関心事をミドルウェアとして分離：

- **Recoverer**: ハンドラ内のpanicをキャッチしてリカバリ
- **Retry**: 失敗時に最大3回再試行（初期間隔100ms）
- Router がAck/Nack、シャットダウンを自動管理

### 3. Dependency Injection

Composition Rootパターンで依存を直接注入：

```go
subscriber, publisher, err := setupInfrastructure(cfg, logger)
application := app.NewApplication(
    subscriber,
    publisher,
    greeter,
    cfg.InputTopic,
    cfg.OutputTopic,
    logger,
)
```

### 4. Repository Pattern

BrokerComponentインターフェースで、メッセージブローカーへのアクセスを抽象化：

```go
type BrokerComponent interface {
    NewSubscriber() (message.Subscriber, error)
    NewPublisher() (message.Publisher, error)
}
```

### 5. Composition Root

`cmd/setup.go`がComposition Rootとして機能し、すべての依存関係を組み立てます：

```go
func setupInfrastructure(cfg *Config, logger watermill.LoggerAdapter) (message.Subscriber, message.Publisher, error)
func setupDomain(cfg *Config) (domain.Greeter, error)
func setupApplication(...) *app.Application
```

## 拡張性

### ブローカーの切り替え

Kafka以外のメッセージブローカー（RabbitMQ、NATS等）への切り替えは、`infra/`配下に新しいパッケージを追加し、`cmd/setup.go`の設定を変更するだけで可能です：

```go
// infra/rabbitmq/rabbitmq.go を作成
// cmd/setup.go で切り替え
func setupInfrastructure(cfg *Config, logger watermill.LoggerAdapter) (message.Subscriber, message.Publisher, error) {
    broker := rabbitmq.NewRabbitMQBroker(...)  // 新
    subscriber, _ := broker.NewSubscriber()
    publisher, _ := broker.NewPublisher()
    return subscriber, publisher, nil
}
```

Application層やDomain層の変更は不要です。

### ミドルウェアの追加

Router パターンにより、横断的関心事をミドルウェアとして簡単に追加できます：

```go
router.AddMiddleware(
    middleware.Recoverer,
    middleware.Retry{...}.Middleware,
    middleware.Throttle(10, time.Second).Middleware,  // 追加
    middleware.Poison{...}.Middleware,                // 追加
)
```

### ビジネスロジックの変更

Domain層のビジネスロジックは他の層から独立しているため、自由に変更・拡張できます。

## まとめ

このアーキテクチャは以下の利点を提供します：

1. **テスタビリティ**: ハンドラ関数を直接テスト可能。各レイヤーが独立してテスト可能
2. **保守性**: 明確な責任分離により、変更の影響範囲が限定的
3. **拡張性**: ミドルウェアやハンドラの追加、技術スタックの変更が容易
4. **信頼性**: Recoverer/Retryミドルウェアによる自動的なエラー回復
5. **依存関係の管理**: DIパターンにより、疎結合を実現
