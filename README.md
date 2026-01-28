# Watermill Kafka Greeting Bridge

[`cmd/main.go`](cmd/main.go:1) を使い、入力トピックの日時ペイロードを解析して時間帯に応じた挨拶メッセージを出力トピックへ publish します。

- Broker: `localhost:9092`
- 入力トピック: `greeting-input`
- 出力トピック: `greeting-output`
- 受け付ける日時フォーマット: RFC3339（例: `{"time":"2024-01-01T09:00:00"}`）
- タイムゾーン: 入力時刻はJSTとして解釈（タイムゾーン省略推奨）

時間帯の振り分け（JST）:
- 04:00-12:00 -> `Good morning!`
- 12:00-18:00 -> `Good afternoon!`
- 18:00-21:00 -> `Good evening!`
- 21:00-04:00 -> `Good night!`

## アーキテクチャとデザインパターン

このプロジェクトは **Domain-Driven Design (DDD)** のレイヤードアーキテクチャと、**ファクトリー関数を使ったDependency Injection (DI)** パターンを採用しています。

### システムアーキテクチャ全体図

```
┌─────────────────────────────────────────────────────────────────┐
│                         cmd/main.go                              │
│                     (Composition Root)                           │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 1. KafkaBroker生成                                         │ │
│  │ 2. ファクトリー関数定義 (Kafka実装をカプセル化)           │ │
│  │ 3. Greeter生成 (ドメインオブジェクト)                     │ │
│  │ 4. Application組み立て                                     │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Application Layer                             │
│                   (app/processor.go)                             │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Application struct                                         │ │
│  │ ┌────────────────────────────────────────────────────┐   │ │
│  │ │ - subscriberFactory: SubscriberFactory            │   │ │
│  │ │ - publisherFactory: PublisherFactory              │   │ │
│  │ │ - greeter: domain.Greeter (interface)             │   │ │
│  │ └────────────────────────────────────────────────────┘   │ │
│  │                                                            │ │
│  │ Run(ctx) メソッド:                                        │ │
│  │ 1. ファクトリー関数からsubscriber/publisher生成          │ │
│  │ 2. メッセージ購読                                         │ │
│  │ 3. ペイロード解析 → ドメインロジック実行 → 結果発行      │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
            │                                      │
            │ (interface)                          │ (factory function)
            ▼                                      ▼
┌──────────────────────────┐    ┌──────────────────────────────────┐
│   Domain Layer           │    │   Infrastructure Layer           │
│ (domain/greeting.go)     │    │   (infra/kafka/)                 │
│ ┌──────────────────────┐ │    │ ┌──────────────────────────────┐ │
│ │ Greeter interface    │ │    │ │ KafkaBroker struct           │ │
│ │                      │ │    │ │                              │ │
│ │ Greet(time) string   │ │    │ │ NewSubscriber() -> Watermill │ │
│ └──────────────────────┘ │    │ │ NewPublisher()  -> Watermill │ │
│ ┌──────────────────────┐ │    │ └──────────────────────────────┘ │
│ │ greeterImpl          │ │    │           │                      │
│ │                      │ │    │           ▼                      │
│ │ 時間帯による         │ │    │ ┌──────────────────────────────┐ │
│ │ 挨拶の振り分け       │ │    │ │ Watermill Kafka Library      │ │
│ └──────────────────────┘ │    │ └──────────────────────────────┘ │
└──────────────────────────┘    └──────────────────────────────────┘
```

### ディレクトリ構造

```
watermillTutorial/
├── cmd/                    # エントリーポイント
│   └── main.go            # アプリケーションの起動
├── app/                    # Application Layer
│   ├── processor.go       # アプリケーションサービス
│   ├── processor_test.go  # ユニットテスト
│   └── time_parser.go     # ペイロード解析
├── domain/                 # Domain Layer
│   ├── greeting.go        # ドメインロジック（挨拶ルール）
│   └── greeting_test.go   # ドメインテスト
├── infra/kafka/           # Infrastructure Layer
│   ├── kafka.go           # Kafka接続実装
│   └── kafka_test.go      # インフラテスト
└── mocks/                 # テスト用モック
    ├── greeting_mock.go
    ├── watermill_mock.go
    └── kafka_mock.go
```

### レイヤードアーキテクチャ

#### 1. Domain Layer ([domain/greeting.go](domain/greeting.go))
- ビジネスルール（時間帯による挨拶の振り分け）を実装
- インフラストラクチャに依存しない純粋なドメインロジック
- `Greeter` インターフェースで抽象化

```go
type Greeter interface {
    Greet(t time.Time) string
}
```

#### 2. Application Layer ([app/processor.go](app/processor.go))
- ドメインロジックとインフラストラクチャを繋ぐ
- メッセージの受信・処理・送信のオーケストレーション
- ファクトリー関数を通じてインフラストラクチャの具体的な実装に依存

```go
type Application struct {
    subscriberFactory SubscriberFactory
    publisherFactory  PublisherFactory
    greeter           domain.Greeter
    inputTopic        string
    outputTopic       string
    logger            watermill.LoggerAdapter
}
```

#### 3. Infrastructure Layer ([infra/kafka/](infra/kafka/))
- Kafka との具体的な通信を実装
- Watermill ライブラリのラッパー
- `BrokerComponent` インターフェースで抽象化

### 採用しているデザインパターン詳細

#### 1. Factory Pattern (ファクトリーパターン)

**目的**: オブジェクトの生成ロジックをカプセル化し、生成の責任を分離する

**実装箇所**: [app/processor.go:14-18](app/processor.go#L14-L18)

```go
// ファクトリー関数型の定義
type SubscriberFactory func() (message.Subscriber, error)
type PublisherFactory func() (message.Publisher, error)
```

**メリット**:
- 生成ロジックの変更が容易（例: Kafka → RabbitMQ への切り替え）
- 生成時のエラーハンドリングを一箇所に集約
- テスト時にモックファクトリーを簡単に差し替え可能

#### 2. Dependency Injection (依存性注入)

**目的**: 依存関係を外部から注入することで、疎結合な設計を実現

**実装箇所**: [cmd/main.go:28-43](cmd/main.go#L28-L43)

```go
// ファクトリー関数を外部から注入
subscriberFactory := func() (message.Subscriber, error) {
    return kafkaBroker.NewSubscriber()
}
publisherFactory := func() (message.Publisher, error) {
    return kafkaBroker.NewPublisher()
}

// Applicationに注入
application := app.NewApplication(
    subscriberFactory,  // ← DI
    publisherFactory,   // ← DI
    greeter,            // ← DI
    inputTopic,
    outputTopic,
    logger,
)
```

**メリット**:
- Applicationクラスは具体的な実装（KafkaBroker）を知らない
- 単体テストでモックを注入可能
- 実行時に異なる実装を切り替え可能

#### 3. Layered Architecture (レイヤードアーキテクチャ)

**目的**: 関心事の分離（Separation of Concerns）により保守性を向上

**依存の方向**:
```
┌─────────────────────────────┐
│     Presentation Layer      │  (cmd/main.go)
│  ・アプリケーションの起動   │
│  ・依存関係の組み立て       │
└─────────────────────────────┘
              ↓ 依存
┌─────────────────────────────┐
│    Application Layer        │  (app/processor.go)
│  ・ユースケースの実装       │
│  ・オーケストレーション     │
└─────────────────────────────┘
              ↓ 依存（interface経由）
┌─────────────────────────────┐
│      Domain Layer           │  (domain/greeting.go)
│  ・ビジネスルール           │
│  ・ドメインロジック         │
│  ・他のレイヤーに依存しない │
└─────────────────────────────┘

┌─────────────────────────────┐
│   Infrastructure Layer      │  (infra/kafka/)
│  ・外部システムとの通信     │
│  ・技術的な実装の詳細       │
└─────────────────────────────┘
        ↑ Factory Function経由で注入
```

**各レイヤーの責務**:

- **Domain Layer**:
  - ビジネスロジックの中核
  - 他のレイヤーに一切依存しない
  - テストが最も簡単

- **Application Layer**:
  - ユースケースの実現
  - ドメインとインフラの橋渡し
  - トランザクション境界の管理

- **Infrastructure Layer**:
  - 外部システム（Kafka）との通信
  - 技術的な詳細の隠蔽
  - 交換可能な設計

- **Presentation Layer (cmd)**:
  - すべての依存関係を組み立てる（Composition Root）
  - アプリケーションの起動と終了

#### 4. Interface Segregation (インターフェース分離)

**目的**: 必要最小限のインターフェースのみに依存する

**実装例**:

```go
// Domain Layer: シンプルなインターフェース
type Greeter interface {
    Greet(t time.Time) string
}

// Application Layer: 必要なメソッドのみ使用
type SubscriberFactory func() (message.Subscriber, error)
type PublisherFactory func() (message.Publisher, error)
```

**メリット**:
- 不要な依存を排除
- モック作成が容易
- インターフェースの変更影響を最小化

#### 5. Composition Root パターン

**目的**: 依存関係の組み立てを一箇所に集約

**実装箇所**: [cmd/main.go](cmd/main.go)

```go
func main() {
    // 1. インフラの初期化
    kafkaBroker := infra.NewKafkaBroker(...)

    // 2. ファクトリー関数の定義
    subscriberFactory := func() { return kafkaBroker.NewSubscriber() }
    publisherFactory := func() { return kafkaBroker.NewPublisher() }

    // 3. ドメインオブジェクトの生成
    greeter, _ := domain.NewGreeter("Asia/Tokyo")

    // 4. Applicationの組み立て
    app := app.NewApplication(subscriberFactory, publisherFactory, greeter, ...)

    // 5. 実行
    app.Run(ctx)
}
```

**メリット**:
- 依存関係のグラフが明確
- 変更時の影響範囲が明確
- アプリケーション全体の構造が理解しやすい

### ファクトリー関数DIパターンの詳細

このプロジェクトでは、**ファクトリー関数を使ったDI** により疎結合な設計を実現しています。

#### なぜファクトリー関数なのか？

1. **遅延生成**: subscriber/publisherの生成を実行時まで遅らせることができる
   - Run()が呼ばれるまでKafka接続を確立しない
   - 必要な時だけリソースを確保

2. **疎結合**: ApplicationはKafkaの具体的な実装を知らない
   - ApplicationはSubscriberFactory/PublisherFactoryという関数型にのみ依存
   - 将来的にKafka以外（RabbitMQ、PubSubなど）への切り替えが容易

3. **テスタビリティ**: モックを返すファクトリー関数で簡単にテスト可能
   - テスト時は実際のKafkaではなくモックを返すファクトリーを注入
   - ネットワーク不要の高速なユニットテスト

4. **リソース管理**: Run()メソッド内で生成し、適切にClose()を呼べる
   - 生成とクリーンアップが同じスコープ内
   - deferで確実にリソース解放

#### 他のDI手法との比較

| 手法 | メリット | デメリット | 採用判断 |
|------|---------|-----------|---------|
| **ファクトリー関数** | シンプル、遅延生成可能 | 関数が増えると煩雑 | ✅ 採用 |
| インターフェース注入 | 型安全、IDEサポート | 事前生成が必要 | - |
| DIコンテナ (wire等) | 大規模で便利 | 小規模では過剰 | - |
| グローバル変数 | 簡単 | テスト困難、疎結合性低 | ❌ |

#### 実装例

[cmd/main.go:28-34](cmd/main.go#L28-L34) でファクトリー関数を定義:

```go
subscriberFactory := func() (message.Subscriber, error) {
    return kafkaBroker.NewSubscriber()
}
publisherFactory := func() (message.Publisher, error) {
    return kafkaBroker.NewPublisher()
}

application := app.NewApplication(
    subscriberFactory,
    publisherFactory,
    greeter,
    inputTopic,
    outputTopic,
    logger,
)
```

[app/processor.go:44-56](app/processor.go#L44-L56) で実行時に生成:

```go
func (a *Application) Run(ctx context.Context) error {
    // ファクトリー関数から生成
    subscriber, err := a.subscriberFactory()
    if err != nil {
        return fmt.Errorf("failed to create subscriber: %w", err)
    }
    defer func() { _ = subscriber.Close() }()

    publisher, err := a.publisherFactory()
    if err != nil {
        return fmt.Errorf("failed to create publisher: %w", err)
    }
    defer func() { _ = publisher.Close() }()
    // ...
}
```

### データフローとシーケンス

#### メッセージ処理フロー

```
Kafka Input Topic                Application                Domain              Kafka Output Topic
(greeting-input)            (processor.go)          (greeting.go)         (greeting-output)
      │                              │                       │                        │
      │  {"time":"2024-01-01T09:00"} │                       │                        │
      ├──────────────────────────────►                       │                        │
      │                              │                       │                        │
      │                              │ parseTimePayload()    │                        │
      │                              │ (time_parser.go)      │                        │
      │                              ├──────────┐            │                        │
      │                              │          │            │                        │
      │                              │◄─────────┘            │                        │
      │                              │ time.Time             │                        │
      │                              │                       │                        │
      │                              │ Greet(time.Time)      │                        │
      │                              ├───────────────────────►                        │
      │                              │                       │ ビジネスロジック実行   │
      │                              │                       │ (時間帯判定)          │
      │                              │                       ├────────────┐          │
      │                              │                       │            │          │
      │                              │                       │◄───────────┘          │
      │                              │                       │                        │
      │                              │  "Good morning!"      │                        │
      │                              │◄──────────────────────┤                        │
      │                              │                       │                        │
      │                              │ Publish()             │                        │
      │                              ├────────────────────────────────────────────────►
      │                              │                       │              "Good morning!"
      │                              │                       │                        │
      │                              │ Ack()                 │                        │
      │                              │                       │                        │
```

#### 起動シーケンス

```
main.go                 KafkaBroker          Application           Kafka Cluster
   │                         │                     │                      │
   │ NewKafkaBroker()        │                     │                      │
   ├────────────────────────►                      │                      │
   │                         │                     │                      │
   │ ファクトリー関数定義    │                     │                      │
   ├──────────────┐          │                     │                      │
   │              │          │                     │                      │
   │◄─────────────┘          │                     │                      │
   │                         │                     │                      │
   │ NewApplication(factories...)                  │                      │
   ├───────────────────────────────────────────────►                      │
   │                         │                     │                      │
   │ Run(ctx)                │                     │                      │
   ├───────────────────────────────────────────────►                      │
   │                         │                     │                      │
   │                         │                     │ subscriberFactory()  │
   │                         │                     ├──────────────┐       │
   │                         │                     │              │       │
   │                         │ NewSubscriber()     │              │       │
   │                         │◄────────────────────┤◄─────────────┘       │
   │                         │                     │                      │
   │                         │ Kafka接続確立       │                      │
   │                         ├──────────────────────────────────────────► │
   │                         │                     │                      │
   │                         │ Subscriber          │                      │
   │                         ├─────────────────────►                      │
   │                         │                     │                      │
   │                         │                     │ publisherFactory()   │
   │                         │                     ├──────────────┐       │
   │                         │                     │              │       │
   │                         │ NewPublisher()      │              │       │
   │                         │◄────────────────────┤◄─────────────┘       │
   │                         │                     │                      │
   │                         │ Kafka接続確立       │                      │
   │                         ├──────────────────────────────────────────► │
   │                         │                     │                      │
   │                         │ Publisher           │                      │
   │                         ├─────────────────────►                      │
   │                         │                     │                      │
   │                         │                     │ Subscribe(topic)     │
   │                         │                     ├──────────────────────►
   │                         │                     │                      │
   │                         │                     │ メッセージ待機開始   │
   │                         │                     │◄─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
   │                         │                     │                      │
```

#### エラーハンドリングフロー

```
Application              parseTimePayload         Greeter              Publisher
    │                           │                     │                     │
    │ メッセージ受信             │                     │                     │
    │                           │                     │                     │
    │ parseTimePayload(payload) │                     │                     │
    ├──────────────────────────►                     │                     │
    │                           │                     │                     │
    │                           │ JSONパースエラー?    │                     │
    │                           ├──────────────┐      │                     │
    │                           │              │      │                     │
    │     error                 │◄─────────────┘      │                     │
    │◄──────────────────────────┤                     │                     │
    │                           │                     │                     │
    │ エラーログ出力             │                     │                     │
    │ out = "unknown"           │                     │                     │
    ├──────────────┐            │                     │                     │
    │              │            │                     │                     │
    │◄─────────────┘            │                     │                     │
    │                           │                     │                     │
    │ Publish("unknown")        │                     │                     │
    ├───────────────────────────────────────────────────────────────────────►
    │                           │                     │                     │
    │                           │                     │            Kafkaエラー?
    │                           │                     │                  ├──┐
    │                           │                     │                  │  │
    │     error                 │                     │                  │◄─┘
    │◄───────────────────────────────────────────────────────────────────────┤
    │                           │                     │                     │
    │ エラーログ出力             │                     │                     │
    │ (メッセージは失われる)     │                     │                     │
    │                           │                     │                     │
    │ Ack() (常に実行)           │                     │                     │
    │                           │                     │                     │
```

**エラーハンドリング戦略**:
- パースエラー: "unknown"を出力トピックに送信
- Publishエラー: ログ出力のみ（メッセージは失われる）
- 常にAck()を実行してメッセージを消費

### テスト戦略

このプロジェクトは **テストピラミッド** に従い、各レイヤーを独立してテストします。

```
        ┌─────────────┐
        │  統合テスト  │  ← 少ない（実際のKafka使用）
        │  (E2E)      │
        └─────────────┘
       ┌──────────────────┐
       │  結合テスト       │  ← 中程度（モック使用）
       │  (Integration)   │
       └──────────────────┘
    ┌────────────────────────┐
    │   ユニットテスト         │  ← 多い（高速、独立）
    │   (Unit Tests)         │
    └────────────────────────┘
```

#### 1. Domain Layer テスト

**ファイル**: [domain/greeting_test.go](domain/greeting_test.go)

**特徴**:
- 外部依存なし
- 純粋なロジックのテスト
- 最も高速で安定

**テストケース例**:

```go
func TestGreeter_Greet(t *testing.T) {
    tests := []struct {
        name     string
        hour     int
        expected string
    }{
        {"朝", 9, "Good morning!"},
        {"昼", 14, "Good afternoon!"},
        {"夜", 20, "Good evening!"},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // テストロジック
        })
    }
}
```

#### 2. Application Layer テスト

**ファイル**: [app/processor_test.go](app/processor_test.go)

**特徴**:
- モックを使用（gomock）
- Kafkaへの実際の接続不要
- ビジネスフローのテスト

**モック構成**:

```go
// モックオブジェクト作成
mockSub := mocks.NewMockSubscriber(ctrl)
mockPub := mocks.NewMockPublisher(ctrl)
mockGreeter := mocks.NewMockGreeter(ctrl)

// 期待する動作を定義
mockSub.EXPECT().Subscribe(gomock.Any(), "input").Return(msgChan, nil)
mockPub.EXPECT().Publish("output", gomock.Any()).Return(nil)
mockGreeter.EXPECT().Greet(gomock.Any()).Return("hello")
mockSub.EXPECT().Close().Return(nil).AnyTimes()
mockPub.EXPECT().Close().Return(nil).AnyTimes()

// ファクトリー関数でモックを返す
subscriberFactory := func() (message.Subscriber, error) {
    return mockSub, nil
}
```

**テストの流れ**:
1. モックファクトリーを注入してApplicationを作成
2. 別goroutineでRun()を実行
3. テストメッセージをチャネルに送信
4. モックが期待通りに呼ばれたか検証

#### 3. Infrastructure Layer テスト

**ファイル**: [infra/kafka/kafka_test.go](infra/kafka/kafka_test.go)

**特徴**:
- KafkaBroker構造体のテスト
- 実際のKafka接続は不要（構造体の生成のみテスト）

**テストケース例**:

```go
func TestNewKafkaBroker(t *testing.T) {
    broker := NewKafkaBroker(
        []string{"localhost:9092"},
        "test-group",
        logger,
    )

    if broker == nil {
        t.Fatal("broker should not be nil")
    }
}
```

#### テストカバレッジ

```bash
# カバレッジレポート生成
go test -coverprofile=coverage.out ./...

# カバレッジ表示
go tool cover -func=coverage.out

# HTML形式で表示
go tool cover -html=coverage.out
```

**目標カバレッジ**:
- Domain Layer: 100%（ロジックが完全にテスト済み）
- Application Layer: 80%以上（主要なフローをカバー）
- Infrastructure Layer: 基本的な動作確認

#### テストの実行順序

1. **ユニットテスト（高速）**:
   ```bash
   go test ./domain/... ./app/... ./infra/...
   ```

2. **並列実行**:
   ```bash
   go test -parallel 4 ./...
   ```

3. **ベンチマーク**:
   ```bash
   go test -bench=. -benchmem ./...
   ```

#### テストデータ管理

**時刻のテストデータ**（タイムゾーン省略、JSTとして解釈）:
```json
// 朝のテストケース (04:00-12:00 JST)
{"time":"2024-01-01T09:00:00"}

// 昼のテストケース (12:00-18:00 JST)
{"time":"2024-01-01T14:00:00"}

// 夕方のテストケース (18:00-21:00 JST)
{"time":"2024-01-01T19:00:00"}

// 夜のテストケース (21:00-04:00 JST)
{"time":"2024-01-01T23:00:00"}
```

#### モック生成

`gomock` を使用してインターフェースのモックを自動生成:

```bash
# モック生成
go generate ./...

# または個別に
mockgen github.com/ThreeDotsLabs/watermill/message Subscriber,Publisher > mocks/watermill_mock.go
```

#### テスト実行

```bash
# すべてのテストを実行
go test ./...

# カバレッジ付き
go test -cover ./...

# 詳細表示
go test -v ./...
```

### 依存関係の方向（Dependency Rule）

```
┌──────────────────────────────────────────────────────────────┐
│                      Composition Root                         │
│                        (cmd/main.go)                          │
│  ・すべての依存関係を組み立てる                               │
│  ・具体的な実装を知る唯一の場所                               │
└──────────────────────────────────────────────────────────────┘
        │                                      │
        │ 依存 (import)                        │ 依存 (import)
        ▼                                      ▼
┌─────────────────────────┐        ┌─────────────────────────────┐
│   Application Layer     │        │  Infrastructure Layer       │
│   (app/processor.go)    │        │  (infra/kafka/)             │
│                         │        │                             │
│  ・ファクトリー関数に    │        │  ・Kafka固有の実装          │
│    依存（interface）    │        │  ・Watermillラッパー        │
│  ・Domainに依存         │        │  ・外部システムとの通信      │
│    （interface）        │        │                             │
└─────────────────────────┘        └─────────────────────────────┘
        │ 依存 (interface)
        ▼
┌─────────────────────────┐
│    Domain Layer         │
│  (domain/greeting.go)   │
│                         │
│  ・他に依存しない        │
│  ・ビジネスロジックの中核│
│  ・最も安定             │
└─────────────────────────┘
```

**依存関係のルール**:
1. **Domain Layer** は他のレイヤーに依存しない（依存ゼロ）
2. **Application Layer** は Domain に依存するが、Infrastructure には直接依存しない
3. **Infrastructure Layer** は独立（他のレイヤーを知らない）
4. **cmd/main.go** がすべてを組み立てる（Composition Root パターン）

**依存性逆転の原則（DIP）の実現**:
```
従来の依存:
Application → Infrastructure (具象クラスに依存) ❌

このプロジェクト:
Application → SubscriberFactory (抽象に依存) ✅
               ↑
         cmd/main.go が具象を注入
```

### 拡張性と保守性

#### 1. 新しいメッセージブローカーへの切り替え

**例: KafkaからRabbitMQへ**

```go
// 新しいInfrastructure実装を追加
// infra/rabbitmq/rabbitmq.go
type RabbitMQBroker struct { ... }

func (r *RabbitMQBroker) NewSubscriber() (message.Subscriber, error) {
    // RabbitMQの実装
}

func (r *RabbitMQBroker) NewPublisher() (message.Publisher, error) {
    // RabbitMQの実装
}

// cmd/main.go で切り替え（Application層は変更不要）
rabbitBroker := rabbitmq.NewRabbitMQBroker(...)

subscriberFactory := func() (message.Subscriber, error) {
    return rabbitBroker.NewSubscriber()  // ← ここだけ変更
}
```

**変更が必要な箇所**:
- ✅ cmd/main.go（Composition Root）のみ
- ❌ app/processor.go（変更不要）
- ❌ domain/greeting.go（変更不要）

#### 2. ビジネスロジックの変更

**例: 挨拶のルールを変更**

```go
// domain/greeting.go のみ変更
func (g *greeterImpl) Greet(t time.Time) string {
    hour := t.In(g.location).Hour()
    switch {
    case hour < 6:
        return "おやすみなさい！"
    case hour < 12:
        return "おはようございます！"
    case hour < 18:
        return "こんにちは！"
    default:
        return "こんばんは！"
    }
}
```

**変更が必要な箇所**:
- ✅ domain/greeting.go のみ
- ❌ app/processor.go（変更不要）
- ❌ cmd/main.go（変更不要）

#### 3. 複数の入力トピックへの対応

```go
// Application層に新しいメソッドを追加
type MultiTopicApplication struct {
    applications map[string]*Application
}

func (m *MultiTopicApplication) Run(ctx context.Context) error {
    // 複数のApplicationを並行実行
    for topic, app := range m.applications {
        go app.Run(ctx)
    }
}
```

#### 4. メトリクス収集の追加

```go
// app/processor.go にメトリクス追加
type Application struct {
    // 既存フィールド
    ...
    metrics MetricsCollector  // 追加
}

func (a *Application) Run(ctx context.Context) error {
    // メッセージ処理後
    a.metrics.IncrementProcessed()
    a.metrics.RecordLatency(duration)
}
```

#### 5. リトライ戦略の追加

```go
// infra/kafka/kafka.go でリトライ追加
func (k *KafkaBroker) NewPublisherWithRetry() (message.Publisher, error) {
    pub, err := kafka.NewPublisher(...)
    if err != nil {
        return nil, err
    }

    // Watermillのミドルウェアでリトライ機能追加
    pub = middleware.Retry{
        MaxRetries:   3,
        InitialInterval: time.Millisecond * 100,
    }.Middleware(pub)

    return pub, nil
}
```

### 保守性を高める設計の原則

#### SOLID原則の適用

| 原則 | 実装箇所 | 説明 |
|------|---------|------|
| **S**ingle Responsibility | 各レイヤー | Domain=ビジネスルール、App=オーケストレーション、Infra=外部通信 |
| **O**pen/Closed | Factory Pattern | 新しい実装を追加しても既存コードを変更しない |
| **L**iskov Substitution | Interface使用 | Greeter, Subscriber, Publisherは交換可能 |
| **I**nterface Segregation | 最小インターフェース | 必要なメソッドのみ定義 |
| **D**ependency Inversion | Factory経由注入 | 抽象に依存、具象に依存しない |

#### 変更の容易さ（Change Ease Matrix）

| 変更内容 | 影響範囲 | 難易度 |
|---------|---------|--------|
| ビジネスルール変更 | Domain層のみ | ⭐ 簡単 |
| メッセージブローカー変更 | cmd/main.go + 新Infra実装 | ⭐⭐ 普通 |
| メッセージフォーマット変更 | time_parser.go | ⭐ 簡単 |
| 並行処理数の調整 | cmd/main.go | ⭐ 簡単 |
| トランザクション追加 | Application層 | ⭐⭐⭐ やや複雑 |

### 主要なデザインパターンまとめ

| パターン | 目的 | 実装箇所 |
|---------|------|---------|
| **Factory Pattern** | オブジェクト生成の抽象化 | SubscriberFactory, PublisherFactory |
| **Dependency Injection** | 依存関係の外部注入 | cmd/main.go → Application |
| **Layered Architecture** | 関心事の分離 | Domain/Application/Infrastructure |
| **Interface Segregation** | 最小限のインターフェース | Greeter, Subscriber, Publisher |
| **Composition Root** | 依存関係の一元管理 | cmd/main.go |
| **Repository Pattern** | データアクセスの抽象化 | BrokerComponent |

セットアップと依存関係:

```bash
# モジュール初期化（既に行っている場合は不要）
go mod init github.com/yourname/watermill-kafka-bridge

# 依存追加
go get github.com/ThreeDotsLabs/watermill
go get github.com/ThreeDotsLabs/watermill-kafka/v2
```

実行:

```bash
# ローカル Kafka が起動している前提で
go run cmd/main.go
```

動作確認（Kafka CLI を使用する例）:

```bash
# producer で日時ペイロードを送る
kafka-console-producer --broker-list localhost:9092 --topic greeting-input
# 例: RFC3339
2026-01-16T06:30:00+09:00
# または Unix秒
1673848200
# または
2026-01-16 06:30:00

# consumer で出力を確認
kafka-console-consumer --bootstrap-server localhost:9092 --topic greeting-output --from-beginning
```

注意:
- 実環境ではブローカーや認証情報を環境変数に置き換えて管理してください。
- エラーハンドリングやリトライ戦略は必要に応じて強化してください。

## Docker Deployment

このプロジェクトはDockerコンテナとしてデプロイできます。KafkaブローカーとアプリケーションをDocker Composeで簡単に起動できます。

### クイックスタート

```bash
# イメージのビルドとサービスの起動
make build
make start

# または一行で
make build && make start
```

### 利用可能なMakeコマンド

| コマンド | 説明 |
|---------|------|
| `make help` | 利用可能なコマンド一覧を表示 |
| `make build` | Dockerイメージをビルド |
| `make start` | サービスを起動（Kafka + アプリ） |
| `make stop` | サービスを停止 |
| `make restart` | サービスを再起動 |
| `make status` | サービスのステータスを表示 |
| `make logs` | すべてのログを表示 |
| `make logs-app` | アプリケーションのログのみ表示 |
| `make logs-kafka` | Kafkaのログのみ表示 |
| `make clean` | すべてのコンテナとボリュームを削除 |

### Kafkaのテスト

#### メッセージの送信（Producer）

```bash
make test-producer
```

起動後、メッセージを入力してEnterキーを押すと、入力トピックにメッセージが送信されます：

```json
{"time":"2024-01-01T09:00:00"}
{"time":"2024-01-01T14:00:00"}
{"time":"2024-01-01T20:00:00"}
{"time":"2024-01-01T02:00:00"}
```

#### メッセージの受信（Consumer）

```bash
make test-consumer
```

出力トピックからメッセージを受信して表示します：

```text
Good morning!
Good afternoon!
Good evening!
Good night!
```

### デプロイ構成

#### docker-compose.yml

- **Kafka**: Bitnami KafkaイメージをKRaftモード（Zookeeper不要）で起動
  - ポート: `9092`
  - 自動トピック作成: 有効
  - ボリューム: `kafka_data`で永続化

- **App**: Goアプリケーション
  - Kafkaのヘルスチェック完了後に起動
  - 環境変数で設定可能

#### Dockerfile

マルチステージビルドを採用：

1. **Build Stage**: Go 1.24でアプリケーションをビルド
2. **Runtime Stage**: Alpine Linuxで軽量なランタイム環境を構築

#### 環境変数

アプリケーションは以下の環境変数で設定できます：

| 環境変数 | デフォルト値 | 説明 |
| ------- | ---------- | ---- |
| `KAFKA_BROKERS` | `kafka:9092` | Kafkaブローカーのアドレス |
| `KAFKA_CONSUMER_GROUP` | `watermill-group` | コンシューマーグループID |
| `INPUT_TOPIC` | `greeting-input` | 入力トピック名 |
| `OUTPUT_TOPIC` | `greeting-output` | 出力トピック名 |
| `TIMEZONE` | `Asia/Tokyo` | タイムゾーン |
| `SHUTDOWN_TIMEOUT` | `30s` | Gracefulシャットダウンのタイムアウト |

[docker-compose.yml](docker-compose.yml)で設定を変更できます。

### トラブルシューティング

#### Kafkaが起動しない

```bash
# Kafkaのログを確認
make logs-kafka

# コンテナの状態を確認
make status
```

#### アプリケーションがKafkaに接続できない

```bash
# アプリケーションのログを確認
make logs-app

# Kafkaのヘルスチェックを確認
docker exec -it watermill-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

#### クリーンな状態から再起動

```bash
# すべてのコンテナとボリュームを削除して再構築
make reset
```

### 本番環境への展開

本番環境では以下の点を考慮してください：

1. **セキュリティ**
   - Kafkaの認証・認可設定を追加
   - ネットワークの分離（プライベートネットワーク）
   - シークレット管理（環境変数を外部から注入）

2. **パフォーマンス**
   - Kafkaのパーティション数を調整
   - コンシューマーグループの並列度を調整
   - リソース制限（CPU、メモリ）を設定

3. **監視**
   - ログ集約（Fluentd、Elasticsearch等）
   - メトリクス収集（Prometheus等）
   - ヘルスチェックの設定

4. **高可用性**
   - Kafkaクラスタの構築（複数ブローカー）
   - アプリケーションの複数インスタンス起動
   - 永続ボリュームのバックアップ

### 開発ワークフロー

```bash
# 開発サイクル
1. コードを修正
2. make rebuild     # 再ビルドして起動
3. make logs-app    # ログを確認
4. make test-producer  # テスト送信
5. make test-consumer  # 結果を確認

# クリーンアップ
make clean
```
