//go:generate mockgen -source=greeting.go -destination=../mocks/greeting_mock.go -package=mocks
package domain

import "time"

// Greeter インターフェース
type Greeter interface {
	Greet(t time.Time) string
}

// 実装構造体
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
func (g *greeterImpl) Greet(t time.Time) string {
	hour := t.In(g.location).Hour()
	switch {
	case hour < 12:
		return "Good morning!"
	case hour < 18:
		return "Good afternoon!"
	default:
		return "Good evening!"
	}
}
