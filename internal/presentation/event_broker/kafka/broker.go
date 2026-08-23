package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
	"user-service/config"
	eventbroker "user-service/internal/presentation/event_broker"
)

type broker struct {
	brokers           []string
	group, deadLetter string
	mu                sync.Mutex
	readers           []*kafka.Reader
	db                *sqlx.DB
}

// NewBroker constructs the Kafka broker used by user-service event adapters.
func NewBroker(cfg config.KafkaConfig) (eventbroker.EventBroker, error) {
	return newBroker(cfg, nil)
}

// NewBrokerWithDB enables durable event claims for user projections.
func NewBrokerWithDB(cfg config.KafkaConfig, db *sqlx.DB) (eventbroker.EventBroker, error) {
	return newBroker(cfg, db)
}

func newBroker(cfg config.KafkaConfig, db *sqlx.DB) (eventbroker.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter", db: db}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, Balancer: &kafka.Hash{}}
	defer w.Close()
	return w.WriteMessages(ctx, kafka.Message{Key: eventKey(payload), Value: payload, Headers: kafkaHeaders(ctx)})
}

func eventKey(payload []byte) []byte { key := sha256.Sum256(payload); return key[:] }

func kafkaHeaders(ctx context.Context) []kafka.Header {
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eventbroker.MessageHandler) error {
	return b.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: subject}, handler)
}

func (b *broker) RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
	group := strings.TrimSpace(b.group) + "-" + strings.TrimSpace(cfg.Subject)
	fmt.Printf("user kafka consumer starting topic=%s group=%s\\n", cfg.Subject, group)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: cfg.Subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("user kafka consumer stopped topic=%s group=%s error=%v\\n", cfg.Subject, group, err)
			return err
		}
		attempts := 0
		err = resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
			attempts = attempt
			var event idempotency.Event
			if b.db != nil {
				event = idempotency.DecodeOrFingerprint(cfg.Subject, msg.Value)
				claimed, claimErr := idempotency.ClaimDB(attemptCtx, b.db, event)
				if claimErr != nil {
					fmt.Printf("user kafka durable claim failed topic=%s offset=%d error=%v\\n", cfg.Subject, msg.Offset, claimErr)
					return claimErr
				}
				if !claimed {
					fmt.Printf("user kafka event already claimed topic=%s offset=%d event_id=%s\\n", cfg.Subject, msg.Offset, event.EventID)
					return nil
				}
			}
			handlerErr := handler(kafkaprop.Context(attemptCtx, msg.Headers), cfg.Subject, msg.Value)
			if handlerErr != nil {
				fmt.Printf("user kafka handler failed topic=%s offset=%d error=%v\\n", cfg.Subject, msg.Offset, handlerErr)
			}
			if handlerErr != nil && b.db != nil {
				_ = idempotency.Release(attemptCtx, b.db, event.EventID)
			}
			return handlerErr
		})
		if err != nil {
			payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: cfg.Subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", err), Error: err.Error(), FailedAt: time.Now().UTC()})
			if marshalErr != nil {
				return marshalErr
			}
			writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
			cancel()
			_ = writer.Close()
			if dlqErr != nil {
				return dlqErr
			}
			sharedmetrics.IncKafkaDLQ(b.deadLetter)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			fmt.Printf("user kafka commit failed topic=%s offset=%d error=%v\\n", cfg.Subject, msg.Offset, err)
			return err
		}
	}
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
