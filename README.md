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

このプロジェクトは **Domain-Driven Design (DDD)** のレイヤードアーキテクチャと、**Watermill Router によるメッセージ処理パイプライン**を採用しています。

### システムアーキテクチャ全体図

```
┌─────────────────────────────────────────────────────────────────┐
│                         cmd/main.go                              │
│                     (Composition Root)                           │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 1. KafkaBroker生成                                         │ │
│  │ 2. Subscriber/Publisher生成                               │ │
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
│  │ │ - subscriber: message.Subscriber                   │   │ │
│  │ │ - publisher: message.Publisher                     │   │ │
│  │ │ - greeter: domain.Greeter (interface)              │   │ │
│  │ └────────────────────────────────────────────────────┘   │ │
│  │                                                            │ │
│  │ Run(ctx) メソッド:                                        │ │
│  │ 1. Watermill Router を生成                                │ │
│  │ 2. ミドルウェア登録 (Recoverer, Retry)                    │ │
│  │ 3. ハンドラ登録 (greeting_handler)                        │ │
│  │ 4. Router.Run(ctx) でメッセージ処理開始                   │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
            │                                      │
            │ (interface)                          │ (直接注入)
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
- Watermill Router によるメッセージ処理パイプラインの構築
- ミドルウェア（Recoverer, Retry）による横断的関心事の宣言的な適用

```go
type Application struct {
    subscriber  message.Subscriber
    publisher   message.Publisher
    greeter     domain.Greeter
    inputTopic  string
    outputTopic string
    logger      watermill.LoggerAdapter
}
```

#### 3. Infrastructure Layer ([infra/kafka/](infra/kafka/))
- Kafka との具体的な通信を実装
- Watermill ライブラリのラッパー
- `BrokerComponent` インターフェースで抽象化

### 採用しているデザインパターン詳細

#### 1. Router Pattern（ルーターパターン）

**目的**: メッセージの受信・処理・送信をパイプラインとして宣言的に定義し、ミドルウェアによる横断的関心事を分離する

**実装箇所**: [app/processor.go:47-72](app/processor.go#L47-L72)

```go
func (a *Application) Run(ctx context.Context) error {
    router, err := message.NewRouter(message.RouterConfig{}, a.logger)

    // ミドルウェアの登録
    router.AddMiddleware(
        middleware.Recoverer,
        middleware.Retry{
            MaxRetries:      3,
            InitialInterval: 100 * time.Millisecond,
            Logger:          a.logger,
        }.Middleware,
    )

    // ハンドラの登録
    router.AddHandler(
        "greeting_handler",
        a.inputTopic, a.subscriber,
        a.outputTopic, a.publisher,
        a.handleMessage,
    )

    return router.Run(ctx)
}
```

**メリット**:
- ハンドラとミドルウェアが明確に分離される
- Recoverer（パニック回復）やRetry（再試行）を宣言的に追加可能
- Router がメッセージのAck/Nack、シャットダウンを自動管理

#### 2. Dependency Injection (依存性注入)

**目的**: 依存関係を外部から注入することで、疎結合な設計を実現

**実装箇所**: [cmd/setup.go](cmd/setup.go)

```go
// インフラ層でSubscriber/Publisherを生成
subscriber, publisher, err := setupInfrastructure(cfg, logger)

// Applicationに直接注入
application := app.NewApplication(
    subscriber,   // ← DI
    publisher,    // ← DI
    greeter,      // ← DI
    cfg.InputTopic,
    cfg.OutputTopic,
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
│  ・Routerによるオーケスト   │
│    レーション               │
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
        ↑ cmd層で生成し、Application層に注入
```

**各レイヤーの責務**:

- **Domain Layer**:
  - ビジネスロジックの中核
  - 他のレイヤーに一切依存しない
  - テストが最も簡単

- **Application Layer**:
  - ユースケースの実現
  - ドメインとインフラの橋渡し
  - Router によるメッセージ処理パイプラインの構築

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

// Infrastructure Layer: ブローカーの抽象化
type BrokerComponent interface {
    NewPublisher() (message.Publisher, error)
    NewSubscriber() (message.Subscriber, error)
}
```

**メリット**:
- 不要な依存を排除
- モック作成が容易
- インターフェースの変更影響を最小化

#### 5. Composition Root パターン

**目的**: 依存関係の組み立てを一箇所に集約

**実装箇所**: [cmd/main.go](cmd/main.go)

```go
func run() error {
    cfg := loadConfig()
    logger := watermill.NewStdLogger(false, false)

    // 1. インフラの初期化（Subscriber/Publisherを直接生成）
    subscriber, publisher, err := setupInfrastructure(cfg, logger)

    // 2. ドメインオブジェクトの生成
    greeter, _ := setupDomain(cfg)

    // 3. Applicationの組み立て
    application := setupApplication(cfg, subscriber, publisher, greeter, logger)

    // 4. シグナルハンドリング（signal.NotifyContext）
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // 5. 実行
    return application.Run(ctx)
}
```

**メリット**:
- 依存関係のグラフが明確
- 変更時の影響範囲が明確
- アプリケーション全体の構造が理解しやすい

### データフローとシーケンス

#### メッセージ処理フロー

```
Kafka Input Topic         Router / Handler            Domain              Kafka Output Topic
(greeting-input)        (processor.go)          (greeting.go)         (greeting-output)
      │                              │                       │                        │
      │  {"time":"2024-01-01T09:00"} │                       │                        │
      ├──────────────────────────────►                       │                        │
      │                              │                       │                        │
      │                    [Middleware: Recoverer]            │                        │
      │                    [Middleware: Retry]                │                        │
      │                              │                       │                        │
      │                              │ handleMessage()       │                        │
      │                              ├──────────┐            │                        │
      │                              │          │            │                        │
      │                              │ parseTimePayload()    │                        │
      │                              │ (time_parser.go)      │                        │
      │                              │◄─────────┘            │                        │
      │                              │ time.Time             │                        │
      │                              │                       │                        │
      │                              │ Greet(time.Time)      │                        │
      │                              ├───────────────────────►                        │
      │                              │                       │ ビジネスロジック実行   │
      │                              │                       │ (時間帯判定)          │
      │                              │  "Good morning!"      │                        │
      │                              │◄──────────────────────┤                        │
      │                              │                       │                        │
      │                              │ return []*Message     │                        │
      │                              ├────────────────────────────────────────────────►
      │                              │                       │              "Good morning!"
      │                              │                       │                        │
      │                              │ [Router が自動 Ack]   │                        │
      │                              │                       │                        │
```

#### 起動シーケンス

```
main.go                 KafkaBroker          Application           Kafka Cluster
   │                         │                     │                      │
   │ NewKafkaBroker()        │                     │                      │
   ├────────────────────────►                      │                      │
   │                         │                     │                      │
   │ NewSubscriber()         │                     │                      │
   ├────────────────────────►                      │                      │
   │                         │ Kafka接続確立       │                      │
   │                         ├──────────────────────────────────────────► │
   │  subscriber             │                     │                      │
   │◄────────────────────────┤                     │                      │
   │                         │                     │                      │
   │ NewPublisher()          │                     │                      │
   ├────────────────────────►                      │                      │
   │                         │ Kafka接続確立       │                      │
   │                         ├──────────────────────────────────────────► │
   │  publisher              │                     │                      │
   │◄────────────────────────┤                     │                      │
   │                         │                     │                      │
   │ NewApplication(subscriber, publisher, ...)    │                      │
   ├───────────────────────────────────────────────►                      │
   │                         │                     │                      │
   │ signal.NotifyContext()  │                     │                      │
   ├──────────────┐          │                     │                      │
   │              │          │                     │                      │
   │◄─────────────┘          │                     │                      │
   │                         │                     │                      │
   │ Run(ctx)                │                     │                      │
   ├───────────────────────────────────────────────►                      │
   │                         │                     │                      │
   │                         │                     │ Router生成           │
   │                         │                     │ ミドルウェア登録     │
   │                         │                     │ ハンドラ登録         │
   │                         │                     │                      │
   │                         │                     │ Router.Run(ctx)      │
   │                         │                     ├──────────────────────►
   │                         │                     │  メッセージ待機開始  │
   │                         │                     │◄─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
   │                         │                     │                      │
```

#### エラーハンドリングフロー

```
Router               handleMessage         Greeter         Middleware
  │                       │                    │                │
  │ メッセージ受信         │                    │                │
  │                       │                    │                │
  │ [Middleware Chain]     │                    │                │
  ├───────────────────────►                    │                │
  │                       │                    │                │
  │                       │ parseTimePayload() │                │
  │                       ├──────────┐         │                │
  │                       │          │         │                │
  │                       │ パースエラー?       │                │
  │                       │◄─────────┘         │                │
  │                       │                    │                │
  │   ┌─────── パース成功の場合 ───────┐       │                │
  │   │                   │           │       │                │
  │   │                   │ Greet()   │       │                │
  │   │                   ├───────────────────►                │
  │   │                   │           │       │                │
  │   │ []*Message        │ greeting  │       │                │
  │   │◄──────────────────┤◄──────────────────┤                │
  │   └───────────────────────────────┘       │                │
  │                       │                    │                │
  │   ┌─────── パース失敗の場合 ───────┐       │                │
  │   │                   │           │       │                │
  │   │ []*Message("unknown")         │       │                │
  │   │◄──────────────────┤           │       │                │
  │   └───────────────────────────────┘       │                │
  │                       │                    │                │
  │ ハンドラがerror返却の場合:                  │                │
  │ ├─────────────────────────────────────────────────────────►│
  │ │                     │                    │    Retry       │
  │ │                     │                    │ (最大3回再試行)│
  │ │◄────────────────────────────────────────────────────────┤
  │                       │                    │                │
  │ 正常完了: Router が自動 Ack                 │                │
  │ 最終失敗: Router が自動 Nack                │                │
  │                       │                    │                │
```

**エラーハンドリング戦略**:
- パースエラー: "unknown"を出力トピックに送信（ハンドラ内で処理）
- ハンドラエラー: Retry ミドルウェアが最大3回再試行
- パニック: Recoverer ミドルウェアがキャッチして安全にリカバリ
- Ack/Nack: Router が自動管理（ハンドラ成功→Ack、最終失敗→Nack）

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
- ハンドラ関数を直接テスト

**テスト構成**:

```go
// モックオブジェクト作成
mockGreeter := mocks.NewMockGreeter(ctrl)

// 期待する動作を定義
mockGreeter.EXPECT().Greet(gomock.Any()).Return("hello")

// Applicationを生成（subscriber/publisherはnilでOK：ハンドラ直接テスト）
a := NewApplication(nil, nil, mockGreeter, "input", "output", logger)

// ハンドラ関数を直接テスト
out, err := a.handleMessage(msg)
```

**テストの流れ**:
1. モックGreeterを注入してApplicationを作成
2. `handleMessage` を直接呼び出し
3. 出力メッセージの内容を検証

#### 3. Infrastructure Layer テスト

**ファイル**: [infra/kafka/kafka_test.go](infra/kafka/kafka_test.go)

**特徴**:
- KafkaBroker構造体のテスト
- 実際のKafka接続は不要（構造体の生成のみテスト）

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

#### テストの実行

```bash
# すべてのテストを実行
go test ./...

# カバレッジ付き
go test -cover ./...

# 詳細表示
go test -v ./...

# ベンチマーク
go test -bench=. -benchmem ./...
```

#### モック生成

`gomock` を使用してインターフェースのモックを自動生成:

```bash
# モック生成
go generate ./...

# または個別に
mockgen github.com/ThreeDotsLabs/watermill/message Subscriber,Publisher > mocks/watermill_mock.go
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
│  ・Watermillインター    │        │  ・Kafka固有の実装          │
│    フェースに依存       │        │  ・Watermillラッパー        │
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
Application → message.Subscriber / message.Publisher (Watermillインターフェースに依存) ✅
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

func (r *RabbitMQBroker) NewSubscriber() (message.Subscriber, error) { ... }
func (r *RabbitMQBroker) NewPublisher() (message.Publisher, error) { ... }

// cmd/setup.go で切り替え（Application層は変更不要）
func setupInfrastructure(...) (message.Subscriber, message.Publisher, error) {
    broker := rabbitmq.NewRabbitMQBroker(...)  // ← ここだけ変更
    subscriber, _ := broker.NewSubscriber()
    publisher, _ := broker.NewPublisher()
    return subscriber, publisher, nil
}
```

**変更が必要な箇所**:
- ✅ cmd/setup.go（Composition Root）と新Infra実装のみ
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

#### 3. ミドルウェアの追加

Router パターンにより、横断的関心事をミドルウェアとして簡単に追加できます:

```go
router.AddMiddleware(
    middleware.Recoverer,                    // パニック回復
    middleware.Retry{...}.Middleware,         // 再試行
    middleware.Throttle(10, time.Second).Middleware,  // スロットリング
    middleware.Poison{...}.Middleware,        // ポイズンキュー
    // 独自ミドルウェアも追加可能
)
```

**変更が必要な箇所**:
- ✅ app/processor.go の `Run()` メソッドのみ

#### 4. 複数ハンドラの追加

```go
// 同一Router上に複数のハンドラを登録可能
router.AddHandler("greeting_handler", inputTopic, sub, outputTopic, pub, a.handleMessage)
router.AddHandler("logging_handler", inputTopic, sub2, logTopic, pub2, a.handleLogging)
```

### 保守性を高める設計の原則

#### SOLID原則の適用

| 原則 | 実装箇所 | 説明 |
|------|---------|------|
| **S**ingle Responsibility | 各レイヤー | Domain=ビジネスルール、App=オーケストレーション、Infra=外部通信 |
| **O**pen/Closed | Router + Middleware | 新しいミドルウェアを追加しても既存コードを変更しない |
| **L**iskov Substitution | Interface使用 | Greeter, Subscriber, Publisherは交換可能 |
| **I**nterface Segregation | 最小インターフェース | 必要なメソッドのみ定義 |
| **D**ependency Inversion | 直接注入 | Watermillインターフェースに依存、具象に依存しない |

#### 変更の容易さ（Change Ease Matrix）

| 変更内容 | 影響範囲 | 難易度 |
|---------|---------|--------|
| ビジネスルール変更 | Domain層のみ | ⭐ 簡単 |
| メッセージブローカー変更 | cmd/setup.go + 新Infra実装 | ⭐⭐ 普通 |
| メッセージフォーマット変更 | time_parser.go | ⭐ 簡単 |
| ミドルウェア追加 | app/processor.go | ⭐ 簡単 |
| ハンドラ追加 | app/processor.go | ⭐ 簡単 |
| トランザクション追加 | Application層 | ⭐⭐⭐ やや複雑 |

### 主要なデザインパターンまとめ

| パターン | 目的 | 実装箇所 |
|---------|------|---------|
| **Router Pattern** | メッセージ処理パイプラインの宣言的定義 | Application.Run() |
| **Middleware Pattern** | 横断的関心事の分離（Retry, Recoverer） | Router.AddMiddleware() |
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
