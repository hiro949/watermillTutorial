package domain

import (
	"testing"
	"time"
)

func TestGreeter_Greet(t *testing.T) {
	// Asia/Tokyo タイムゾーンで Greeter を生成
	greeter, err := NewGreeter("Asia/Tokyo")
	if err != nil {
		t.Fatalf("failed to create greeter: %v", err)
	}

	tests := []struct {
		name     string
		hour     int
		expected string
	}{
		// Good night: 21:00 JST - 4:00 JST
		{"3時はGood night", 3, "Good night!"},
		// Good morning: 4:00 JST - 12:00 JST
		{"4時はGood morning", 4, "Good morning!"},
		{"9時はGood morning", 9, "Good morning!"},
		{"11時はGood morning", 11, "Good morning!"},
		// Good afternoon: 12:00 JST - 18:00 JST
		{"12時はGood afternoon", 12, "Good afternoon!"},
		{"15時はGood afternoon", 15, "Good afternoon!"},
		{"17時はGood afternoon", 17, "Good afternoon!"},
		// Good evening: 18:00 JST - 21:00 JST
		{"18時はGood evening", 18, "Good evening!"},
		{"20時はGood evening", 20, "Good evening!"},
		// Good night: 21:00 JST - 4:00 JST
		{"21時はGood night", 21, "Good night!"},
		{"23時はGood night", 23, "Good night!"},
		{"0時はGood night", 0, "Good night!"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 入力時刻はJSTとして解釈される
			inputTime := time.Date(2026, 1, 16, tt.hour, 0, 0, 0, time.UTC)
			got := greeter.Greet(inputTime)
			if got != tt.expected {
				t.Errorf("hour %d: expected %q, got %q", tt.hour, tt.expected, got)
			}
		})
	}
}
