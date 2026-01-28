// Package kafka provides Kafka publisher/subscriber implementations.
//
//go:generate mockgen -source=kafka.go -destination=../../mocks/kafka_mock.go -package=mocks
package kafka

import (
	"log"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// BrokerComponent は publish/subscribe の抽象を表すインターフェースです。
type BrokerComponent interface {
	NewPublisher() (message.Publisher, error)
	NewSubscriber() (message.Subscriber, error)
}

// KafkaBroker は Kafka ベースの実装です。
type KafkaBroker struct {
	Brokers []string
	GroupID string
	Logger  watermill.LoggerAdapter
}

func NewKafkaBroker(brokers []string, groupID string, logger watermill.LoggerAdapter) *KafkaBroker {
	return &KafkaBroker{Brokers: brokers, GroupID: groupID, Logger: logger}
}

func (k *KafkaBroker) NewPublisher() (message.Publisher, error) {
	config := kafka.PublisherConfig{
		Brokers:   k.Brokers,
		Marshaler: kafka.DefaultMarshaler{},
	}

	pub, err := kafka.NewPublisher(config, k.Logger)
	if err != nil {
		log.Printf("kafka NewPublisher error: %v", err)
	}
	return pub, err
}

func (k *KafkaBroker) NewSubscriber() (message.Subscriber, error) {
	config := kafka.SubscriberConfig{
		Brokers:       k.Brokers,
		ConsumerGroup: k.GroupID,
		Unmarshaler:   kafka.DefaultMarshaler{},
	}

	sub, err := kafka.NewSubscriber(config, k.Logger)
	if err != nil {
		log.Printf("kafka NewSubscriber error: %v", err)
	}
	return sub, err
}
