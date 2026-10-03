package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
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
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, Balancer: &kafka.Hash{}, BatchSize: 100, BatchTimeout: 50 * time.Millisecond}
	defer w.Close()
	transportkafka.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Key: eventKey(enveloped), Value: enveloped, Headers: kafkaHeaders(ctx)})
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
	group := strings.TrimSpace(cfg.GroupID)
	if group == "" {
		group = strings.TrimSpace(b.group) + "-" + strings.TrimSpace(cfg.Subject)
	}
	fmt.Printf("user kafka consumer starting topic=%s group=%s\\n", cfg.Subject, group)
	go func() {
		_ = (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: group, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx)
	}()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: cfg.Subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6, MaxWait: 50 * time.Millisecond})
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
		attempts := retryAttempt(msg.Headers)
		var event idempotency.Event
		if b.db != nil {
			event = idempotency.DecodeOrFingerprint(cfg.Subject, msg.Value)
			claimed, claimErr := idempotency.ClaimDB(ctx, b.db, event)
			if claimErr != nil {
				return claimErr
			}
			if !claimed {
				continue
			}
		}
		payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
		if unwrapErr != nil {
			return b.deadLetterMessage(ctx, cfg.Subject, msg, attempts, unwrapErr)
		}
		transportkafka.Consumed(cfg.Subject, msg.Partition, msg.Offset, attempts, payload)
		if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
			payload = msg.Value
		}
		err = handler(kafkaprop.Context(ctx, msg.Headers), cfg.Subject, payload)
		if err != nil {
			if b.db != nil {
				_ = idempotency.Release(ctx, b.db, event.EventID)
			}
			var permanent resilience.PermanentError
			if !errors.As(err, &permanent) && attempts < resilience.DefaultRetryPolicy.MaxAttempts {
				writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(group), WriteTimeout: 5 * time.Second}
				queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(ctx, msg, cfg.Subject, attempts+1, err)
				_ = writer.Close()
				if queueErr != nil {
					return queueErr
				}
				continue
			}
			return b.deadLetterMessage(ctx, cfg.Subject, msg, attempts, err)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			fmt.Printf("user kafka commit failed topic=%s offset=%d error=%v\\n", cfg.Subject, msg.Offset, err)
			return err
		}
	}
}

func retryAttempt(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == "x-ofm-retry-attempt" {
			if n, err := strconv.Atoi(string(h.Value)); err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

func (b *broker) deadLetterMessage(ctx context.Context, subject string, msg kafka.Message, attempts int, cause error) error {
	payload, err := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", cause), Error: cause.Error(), FailedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
	cancel()
	_ = writer.Close()
	if err != nil {
		return err
	}
	sharedmetrics.IncKafkaDLQ(b.deadLetter)
	return nil
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
