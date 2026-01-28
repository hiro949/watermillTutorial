//go:generate mockgen -source=processor.go -destination=../mocks/processor_mock.go -package=mocks
package app

import (
	"encoding/json"
	"time"
)

type TimePayload struct {
	Time string `json:"time"`
}

// サポートする日時フォーマット
var timeFormats = []string{
	"2006-01-02T15:04:05",       // タイムゾーン省略（推奨）
	time.RFC3339,                // RFC3339（タイムゾーン付き）
	"2006-01-02 15:04:05",       // スペース区切り
	"2006-01-02T15:04:05Z07:00", // RFC3339変形
}

func parseTimePayload(payload []byte) (time.Time, error) {
	var tp TimePayload
	if err := json.Unmarshal(payload, &tp); err != nil {
		return time.Time{}, err
	}

	// 複数のフォーマットを試行
	for _, format := range timeFormats {
		if t, err := time.Parse(format, tp.Time); err == nil {
			return t, nil
		}
	}

	// 全て失敗した場合は最初のフォーマットでエラーを返す
	return time.Parse(timeFormats[0], tp.Time)
}
