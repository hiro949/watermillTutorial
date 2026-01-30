//go:generate mockgen -source=greeting.go -destination=../mocks/greeting_mock.go -package=mocks
package domain

import "time"

// 挨拶メッセージ
const (
	goodMorningMessage   = "Good morning!"
	goodAfternoonMessage = "Good afternoon!"
	goodEveningMessage   = "Good evening!"
	goodNightMessage     = "Good night!"
)

// 時間帯の境界（JST）
const (
	morningStartHour   = 4  // Good morning 開始
	afternoonStartHour = 12 // Good afternoon 開始
	eveningStartHour   = 18 // Good evening 開始
	nightStartHour     = 21 // Good night 開始
)

// timePeriod は時間帯と対応するメッセージを定義
type timePeriod struct {
	startHour int
	endHour   int
	message   string
}

func getTimePeriods() []timePeriod {
	return []timePeriod{
		{morningStartHour, afternoonStartHour, goodMorningMessage},
		{afternoonStartHour, eveningStartHour, goodAfternoonMessage},
		{eveningStartHour, nightStartHour, goodEveningMessage},
	}
}

// Greeter インターフェース
type Greeter interface {
	Greet(t time.Time) string
}

// greeterImpl は Greeter の実装
type greeterImpl struct {
	location *time.Location
}

// NewGreeter は指定したタイムゾーンで Greeter を生成する
func NewGreeter(tz string) (Greeter, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	return &greeterImpl{location: loc}, nil
}

// Greet は時刻に応じて挨拶を返す
// 入力時刻はJSTとして解釈される
func (g *greeterImpl) Greet(t time.Time) string {
	hour := g.extractHour(t)

	for _, period := range getTimePeriods() {
		if hour >= period.startHour && hour < period.endHour {
			return period.message
		}
	}
	return goodNightMessage
}

// extractHour は入力時刻をJSTとして解釈し、時間を返す
func (g *greeterImpl) extractHour(t time.Time) int {
	jstTime := time.Date(
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		g.location,
	)
	return jstTime.Hour()
}
