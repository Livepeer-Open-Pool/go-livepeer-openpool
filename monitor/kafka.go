package monitor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/golang/glog"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	KafkaBatchInterval  = 1 * time.Second
	KafkaRequestTimeout = 60 * time.Second
	KafkaBatchSize      = 100
	KafkaChannelSize    = 100
)

var (
	// ErrKafkaProducerNotConfigured is returned when no Kafka producer has been started.
	ErrKafkaProducerNotConfigured = errors.New("kafka producer not configured")
	// ErrEventQueueFull is returned when an event cannot be enqueued because the queue is full.
	ErrEventQueueFull = errors.New("kafka event queue full")
)

// KafkaWriter is the subset of kafka.Writer used by the producer.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type KafkaProducer struct {
	writer         KafkaWriter
	topic          string
	events         chan GatewayEvent
	reserved       chan GatewayEvent // events sent through an EventReservation
	slots          chan struct{}     // one token per reservation held or waiting in reserved
	gatewayAddress string
}

type GatewayEvent struct {
	ID        *string     `json:"id,omitempty"`
	Type      *string     `json:"type"`
	Timestamp *string     `json:"timestamp"`
	Gateway   *string     `json:"gateway,omitempty"`
	Data      interface{} `json:"data"`
}

type PipelineStatus struct {
	Pipeline             string      `json:"pipeline"`
	StartTime            float64     `json:"start_time"`
	LastParamsUpdateTime float64     `json:"last_params_update_time"`
	LastParams           interface{} `json:"last_params"`
	LastParamsHash       string      `json:"last_params_hash"`
	InputFPS             float64     `json:"input_fps"`
	OutputFPS            float64     `json:"output_fps"`
	LastInputTime        float64     `json:"last_input_time"`
	LastOutputTime       float64     `json:"last_output_time"`
	RestartCount         int         `json:"restart_count"`
	LastRestartTime      float64     `json:"last_restart_time"`
	LastRestartLogs      []string    `json:"last_restart_logs"`
	LastError            *string     `json:"last_error"`
	StreamID             *string     `json:"stream_id"`
}

var kafkaProducer *KafkaProducer

func InitKafkaProducer(bootstrapServers, user, password, topic, gatewayAddress string) error {
	writer, err := newKafkaWriter(bootstrapServers, user, password, topic)
	if err != nil {
		return err
	}
	producer := NewKafkaProducer(writer, topic, gatewayAddress)
	SetKafkaProducer(producer)
	go producer.Run(context.Background())
	return nil
}

// NewKafkaProducer creates a producer that publishes events through writer.
// The producer does not consume its queue until Run is called.
func NewKafkaProducer(writer KafkaWriter, topic, gatewayAddress string) *KafkaProducer {
	return &KafkaProducer{
		writer:         writer,
		topic:          topic,
		events:         make(chan GatewayEvent, KafkaChannelSize),
		reserved:       make(chan GatewayEvent, KafkaChannelSize),
		slots:          make(chan struct{}, KafkaChannelSize),
		gatewayAddress: gatewayAddress,
	}
}

// SetKafkaProducer installs producer as the process-wide event producer.
// A nil producer disables event publishing.
func SetKafkaProducer(producer *KafkaProducer) {
	kafkaProducer = producer
}

func newKafkaWriter(bootstrapServers, user, password, topic string) (KafkaWriter, error) {
	dialer := &kafka.Dialer{
		Timeout:   KafkaRequestTimeout,
		DualStack: true,
	}

	if user != "" && password != "" {
		tls := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		sasl := &plain.Mechanism{
			Username: user,
			Password: password,
		}
		dialer.SASLMechanism = sasl
		dialer.TLS = tls
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{bootstrapServers},
		Topic:    topic,
		Balancer: kafka.CRC32Balancer{},
		Dialer:   dialer,
	})

	return writer, nil
}

// QueueLength returns the number of events waiting to be batched.
func (p *KafkaProducer) QueueLength() int {
	return len(p.events) + len(p.reserved)
}

func (p *KafkaProducer) reportQueueLength() {
	KafkaEventQueueLength(p.QueueLength())
}

// Run consumes the event queue and publishes batches until ctx is done.
func (p *KafkaProducer) Run(ctx context.Context) {
	ticker := time.NewTicker(KafkaBatchInterval)
	defer ticker.Stop()

	var eventsBatch []kafka.Message

	for {
		select {
		case event := <-p.events:
			p.reportQueueLength()
			eventsBatch = p.appendEvent(eventsBatch, event)

		case event := <-p.reserved:
			<-p.slots
			p.reportQueueLength()
			eventsBatch = p.appendEvent(eventsBatch, event)

		case <-ticker.C:
			if len(eventsBatch) > 0 {
				p.sendBatch(eventsBatch)
				eventsBatch = nil
			}

		case <-ctx.Done():
			return
		}
	}
}

func (p *KafkaProducer) appendEvent(eventsBatch []kafka.Message, event GatewayEvent) []kafka.Message {
	value, err := json.Marshal(event)
	if err != nil {
		glog.Errorf("error while marshalling gateway log to Kafka, err=%v", err)
		return eventsBatch
	}

	msg := kafka.Message{
		Key:   []byte(*event.ID),
		Value: value,
	}
	eventsBatch = append(eventsBatch, msg)

	// Send batch if it reaches the defined size
	if len(eventsBatch) >= KafkaBatchSize {
		p.sendBatch(eventsBatch)
		eventsBatch = nil
	}
	return eventsBatch
}

func (p *KafkaProducer) sendBatch(eventsBatch []kafka.Message) {
	// We retry sending messages to Kafka in case of a failure
	kafkaWriteRetries := 3
	var writeErr error
	for i := 0; i < kafkaWriteRetries; i++ {
		writeErr = p.writer.WriteMessages(context.Background(), eventsBatch...)
		if writeErr == nil {
			return
		}
		glog.Warningf("error while sending gateway log batch to Kafka, retrying, topic=%s, try=%d, err=%v", p.topic, i, writeErr)
	}
	if writeErr != nil {
		glog.Errorf("error while sending gateway log batch to Kafka, the gateway logs are lost, err=%v", writeErr)
	}
}

func (p *KafkaProducer) newEvent(eventType string, data interface{}) GatewayEvent {
	randomID := uuid.New().String()
	timestampMs := time.Now().UnixMilli()

	return GatewayEvent{
		ID:        stringPtr(randomID),
		Gateway:   stringPtr(p.gatewayAddress),
		Type:      &eventType,
		Timestamp: stringPtr(fmt.Sprint(timestampMs)),
		Data:      data,
	}
}

// SendQueueEventAsync enqueues an event without blocking. It returns
// ErrKafkaProducerNotConfigured when no producer is running and
// ErrEventQueueFull when the event was dropped because the queue is full.
func SendQueueEventAsync(eventType string, data interface{}) error {
	p := kafkaProducer
	if p == nil {
		return ErrKafkaProducerNotConfigured
	}

	select {
	case p.events <- p.newEvent(eventType, data):
		p.reportQueueLength()
		return nil
	default:
		glog.Warningf("kafka producer event queue is full, dropping event %q", eventType)
		KafkaEventSendError(eventType)
		return ErrEventQueueFull
	}
}

// EventReservation is a held slot in the event queue. Sending through it
// never blocks and never drops the event.
type EventReservation struct {
	producer *KafkaProducer
	done     bool
}

// ReserveQueueEvent waits up to timeout for a free slot in the event queue.
// The caller must either Send or Release the reservation. It returns
// ErrKafkaProducerNotConfigured when no producer is running, ErrEventQueueFull
// when no slot frees up within timeout, and the context error when ctx ends.
func ReserveQueueEvent(ctx context.Context, timeout time.Duration) (*EventReservation, error) {
	p := kafkaProducer
	if p == nil {
		return nil, ErrKafkaProducerNotConfigured
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case p.slots <- struct{}{}:
		return &EventReservation{producer: p}, nil
	case <-timer.C:
		return nil, ErrEventQueueFull
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Send enqueues the event in the reserved slot. It is a no-op once the
// reservation has been used or released.
func (r *EventReservation) Send(eventType string, data interface{}) {
	if r == nil || r.done {
		return
	}
	r.done = true
	// Never blocks: the number of events in reserved cannot exceed the
	// number of held slots, and this reservation holds one of them.
	r.producer.reserved <- r.producer.newEvent(eventType, data)
	r.producer.reportQueueLength()
}

// Release frees the reserved slot without sending an event. It is a no-op
// once the reservation has been used or released.
func (r *EventReservation) Release() {
	if r == nil || r.done {
		return
	}
	r.done = true
	<-r.producer.slots
}

func stringPtr(s string) *string {
	return &s
}
