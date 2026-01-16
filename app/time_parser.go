//go:generate mockgen -source=processor.go -destination=../mocks/processor_mock.go -package=mocks
package app

import (
	"encoding/json"
	"time"
)

type TimePayload struct {
	Time string `json:"time"`
}

func parseTimePayload(payload []byte) (time.Time, error) {
	var tp TimePayload
	if err := json.Unmarshal(payload, &tp); err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, tp.Time)
}
