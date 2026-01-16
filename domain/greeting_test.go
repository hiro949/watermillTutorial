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

	// テスト用の時刻を設定（午前9時）
	now := time.Date(2026, 1, 16, 9, 0, 0, 0, time.Local)

	got := greeter.Greet(now)
	if got == "" {
		t.Errorf("expected non-empty greeting, got %q", got)
	}
}
