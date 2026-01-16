# アーキテクチャ設計

## レイヤー構造

本プロジェクトはクリーンアーキテクチャに基づいた3層構造を採用しています。

```
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

```
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
│   ├── processor.go       # メッセージ処理ロジック
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
- メッセージ処理フローのオーケストレーション

**特徴:**
- Domain層に依存
- Watermillインターフェースに依存（抽象に依存）
- 具体的なインフラ実装には依存しない（DIパターン）

**含まれるもの:**
- `Application`: メッセージ処理アプリケーション
- `SubscriberFactory`: Subscriber生成の抽象化
- `PublisherFactory`: Publisher生成の抽象化

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
- Factory関数パターンで依存を注入

**含まれるもの:**
- `main.go`: エントリーポイント
- `config.go`: 環境変数からの設定読み込み
- `setup.go`: 各レイヤーのセットアップ関数

## 依存関係のルール

### 依存の方向

```
cmd → app → domain
      ↑
      │ (依存の逆転)
      │
    infra
```

1. **内側への依存のみ許可**: 外側の層は内側の層に依存できますが、内側の層は外側の層に依存できません
2. **抽象への依存**: Application層はInfrastructure層の具体実装ではなく、インターフェース（抽象）に依存します
3. **依存性の注入**: 具体的な実装はComposition Root（cmd層）で組み立てられます

### 依存性逆転の原則（DIP）の実現

```go
// app層: 抽象に依存
type SubscriberFactory func() (message.Subscriber, error)
type PublisherFactory func() (message.Publisher, error)

// cmd層: 具体実装を注入
subscriberFactory := func() (message.Subscriber, error) {
    return kafkaBroker.NewSubscriber()
}
```

Application層は`message.Subscriber`というインターフェースに依存し、Kafka実装の詳細を知りません。

## テスト戦略

### 単体テスト

各レイヤーは独立してテスト可能です：

- **Domain層**: 純粋関数のテストが中心
- **Application層**: モックを使用したユースケーステスト
- **Infrastructure層**: 実際の依存を使用した統合テスト

### モックの使用

`mocks/`ディレクトリには、gomockで生成されたモックが配置されています：

```go
// テスト例
ctrl := gomock.NewController(t)
defer ctrl.Finish()

mockGreeter := mocks.NewMockGreeter(ctrl)
mockGreeter.EXPECT().Greet("User").Return("Hello, User!")
```

## デザインパターン

### 1. Factory Pattern

Factory関数を使用して、オブジェクトの生成を抽象化：

```go
type SubscriberFactory func() (message.Subscriber, error)
```

### 2. Dependency Injection

Composition Rootパターンで依存を注入：

```go
application := app.NewApplication(
    subscriberFactory,
    publisherFactory,
    greeter,
    inputTopic,
    outputTopic,
    logger,
)
```

### 3. Repository Pattern

BrokerComponentインターフェースで、メッセージブローカーへのアクセスを抽象化：

```go
type BrokerComponent interface {
    NewSubscriber() (message.Subscriber, error)
    NewPublisher() (message.Publisher, error)
}
```

### 4. Composition Root

`cmd/setup.go`がComposition Rootとして機能し、すべての依存関係を組み立てます：

```go
func setupInfrastructure(cfg *Config, logger watermill.LoggerAdapter) (...)
func setupDomain(cfg *Config) (...)
func setupApplication(...) *app.Application
```

## 拡張性

### ブローカーの切り替え

Kafka以外のメッセージブローカー（RabbitMQ、NATS等）への切り替えは、`infra/`配下に新しいパッケージを追加し、`cmd/setup.go`の設定を変更するだけで可能です：

```go
// infra/rabbitmq/rabbitmq.go を作成
// cmd/setup.go で切り替え
func setupInfrastructure(cfg *Config, logger watermill.LoggerAdapter) (...) {
    // kafkaBroker := kafka.NewKafkaBroker(...)  // 旧
    rabbitmqBroker := rabbitmq.NewRabbitMQBroker(...)  // 新

    subscriberFactory := func() (message.Subscriber, error) {
        return rabbitmqBroker.NewSubscriber()
    }
    // ...
}
```

Application層やDomain層の変更は不要です。

### ビジネスロジックの変更

Domain層のビジネスロジックは他の層から独立しているため、自由に変更・拡張できます。

## まとめ

このアーキテクチャは以下の利点を提供します：

1. **テスタビリティ**: 各レイヤーが独立してテスト可能
2. **保守性**: 明確な責任分離により、変更の影響範囲が限定的
3. **拡張性**: 新機能の追加や技術スタックの変更が容易
4. **可読性**: レイヤー構造により、コードの役割が明確
5. **依存関係の管理**: DIパターンにより、疎結合を実現
