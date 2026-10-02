package monitor

import (
	"context"
	"encoding/json"
	"time"
	"uuid"

	"github.com/golang/glog"
	"github.com/segmentio/kafka-go"
)

// Orchestrator events are the record a pool service settles from: which Live
// Runner registered under which member, which sessions it served, and every
// fee it earned. They go to their own topic through their own queue, apart
// from the gateway's events, as flat JSON objects keyed by runner so that one
// runner's events stay in order.

// OrchestratorKafkaChannelSize is how many orchestrator events may wait to be batched.
const OrchestratorKafkaChannelSize = 1000

// OrchestratorKafkaProducer publishes orchestrator events.
type OrchestratorKafkaProducer struct {
	writer KafkaWriter
	topic  string
	events chan kafka.Message
	// enqueueTimeout is how long a send waits for room in a full queue before
	// the event is dropped; zero does not wait.
	enqueueTimeout time.Duration
}

var orchestratorKafkaProducer *OrchestratorKafkaProducer

// InitOrchestratorKafkaProducer starts the producer for the orchestrator topic.
func InitOrchestratorKafkaProducer(bootstrapServers, user, password, topic string, enqueueTimeout time.Duration) error {
	writer, err := newKafkaWriter(bootstrapServers, user, password, topic)
	if err != nil {
		return err
	}
	producer := NewOrchestratorKafkaProducer(writer, topic, enqueueTimeout)
	SetOrchestratorKafkaProducer(producer)
	go producer.Run(context.Background())
	return nil
}

// NewOrchestratorKafkaProducer creates a producer that publishes through
// writer. It does not consume its queue until Run is called.
func NewOrchestratorKafkaProducer(writer KafkaWriter, topic string, enqueueTimeout time.Duration) *OrchestratorKafkaProducer {
	return &OrchestratorKafkaProducer{
		writer:         writer,
		topic:          topic,
		events:         make(chan kafka.Message, OrchestratorKafkaChannelSize),
		enqueueTimeout: enqueueTimeout,
	}
}

// SetOrchestratorKafkaProducer installs the process-wide orchestrator event
// producer. A nil producer disables orchestrator events.
func SetOrchestratorKafkaProducer(producer *OrchestratorKafkaProducer) {
	orchestratorKafkaProducer = producer
}

// OrchestratorEventsEnabled says whether orchestrator events are published.
func OrchestratorEventsEnabled() bool {
	return orchestratorKafkaProducer != nil
}

// QueueLength returns the number of events waiting to be batched.
func (p *OrchestratorKafkaProducer) QueueLength() int {
	return len(p.events)
}

// Run consumes the event queue and publishes batches until ctx is done.
func (p *OrchestratorKafkaProducer) Run(ctx context.Context) {
	ticker := time.NewTicker(KafkaBatchInterval)
	defer ticker.Stop()

	var batch []kafka.Message
	for {
		select {
		case msg := <-p.events:
			batch = append(batch, msg)
			if len(batch) >= KafkaBatchSize {
				p.sendBatch(batch)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.sendBatch(batch)
				batch = nil
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *OrchestratorKafkaProducer) sendBatch(batch []kafka.Message) {
	kafkaWriteRetries := 3
	var writeErr error
	for i := 0; i < kafkaWriteRetries; i++ {
		writeErr = p.writer.WriteMessages(context.Background(), batch...)
		if writeErr == nil {
			return
		}
		glog.Warningf("error while sending orchestrator event batch to Kafka, retrying, topic=%s, try=%d, err=%v", p.topic, i, writeErr)
	}
	for _, msg := range batch {
		// The events are the pool's accounting record: leave them in the log.
		glog.Errorf("orchestrator event lost, Kafka write failed topic=%s err=%v event=%s", p.topic, writeErr, msg.Value)
	}
}

// SendOrchestratorEvent publishes one event on the orchestrator topic: a JSON
// object of fields plus event_type, an event_id that identifies it across
// redeliveries, and ts_ms when the caller did not set it. key orders events
// that share it. It returns ErrKafkaProducerNotConfigured when orchestrator
// events are off and ErrEventQueueFull when the event was dropped; a dropped
// event is counted in orchestrator_events_dropped_total.
func SendOrchestratorEvent(eventType, key string, fields map[string]interface{}) error {
	p := orchestratorKafkaProducer
	if p == nil {
		return ErrKafkaProducerNotConfigured
	}
	event := make(map[string]interface{}, len(fields)+3)
	for k, v := range fields {
		event[k] = v
	}
	event["event_type"] = eventType
	event["event_id"] = uuid.New().String()
	if _, ok := event["ts_ms"]; !ok {
		event["ts_ms"] = time.Now().UnixMilli()
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := kafka.Message{Key: []byte(key), Value: value}

	select {
	case p.events <- msg:
		return nil
	default:
	}
	if p.enqueueTimeout > 0 {
		timer := time.NewTimer(p.enqueueTimeout)
		defer timer.Stop()
		select {
		case p.events <- msg:
			return nil
		case <-timer.C:
		}
	}
	OrchestratorEventDropped(eventType)
	return ErrEventQueueFull
}
