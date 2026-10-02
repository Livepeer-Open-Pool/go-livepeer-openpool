package monitor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

type fakeKafkaWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
}

func (w *fakeKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func (w *fakeKafkaWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.msgs)
}

func installTestKafkaProducer(t *testing.T) (*KafkaProducer, *fakeKafkaWriter) {
	t.Helper()
	writer := &fakeKafkaWriter{}
	producer := NewKafkaProducer(writer, "topic", "gateway")
	SetKafkaProducer(producer)
	t.Cleanup(func() { SetKafkaProducer(nil) })
	return producer, writer
}

func fillEventQueue(t *testing.T) {
	t.Helper()
	for i := 0; i < KafkaChannelSize; i++ {
		require.NoError(t, SendQueueEventAsync("fill", i))
	}
}

func holdAllReservations(t *testing.T) []*EventReservation {
	t.Helper()
	held := make([]*EventReservation, 0, KafkaChannelSize)
	for i := 0; i < KafkaChannelSize; i++ {
		res, err := ReserveQueueEvent(context.Background(), time.Second)
		require.NoError(t, err)
		held = append(held, res)
	}
	t.Cleanup(func() {
		for _, res := range held {
			res.Release()
		}
	})
	return held
}

// ensureTestCensus initialises census metrics once per test binary (views
// cannot be registered twice) and enables recording for the test.
func ensureTestCensus(t *testing.T) {
	t.Helper()
	if census.mRemoteSignerEventQueueLength == nil {
		unitTestMode = true
		defer func() { unitTestMode = false }()
		NodeID = "testid"
		InitCensus(Broadcaster, "testversion")
	}
	Enabled = true
	t.Cleanup(func() { Enabled = false })
}

func lastGaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	rows, err := view.RetrieveData(name)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0].Data.(*view.LastValueData).Value
}

func TestSendQueueEventAsync_NotConfigured(t *testing.T) {
	SetKafkaProducer(nil)
	require.ErrorIs(t, SendQueueEventAsync("evt", nil), ErrKafkaProducerNotConfigured)
}

func TestSendQueueEventAsync_DropsWhenQueueFull(t *testing.T) {
	producer, _ := installTestKafkaProducer(t)
	fillEventQueue(t)

	require.ErrorIs(t, SendQueueEventAsync("evt", nil), ErrEventQueueFull)
	require.Equal(t, KafkaChannelSize, producer.QueueLength())
}

func TestReserveQueueEvent_NotConfigured(t *testing.T) {
	SetKafkaProducer(nil)
	_, err := ReserveQueueEvent(context.Background(), time.Second)
	require.ErrorIs(t, err, ErrKafkaProducerNotConfigured)
}

func TestReserveQueueEvent_TimesOutWhenQueueFull(t *testing.T) {
	installTestKafkaProducer(t)
	holdAllReservations(t)

	start := time.Now()
	res, err := ReserveQueueEvent(context.Background(), 50*time.Millisecond)
	require.ErrorIs(t, err, ErrEventQueueFull)
	require.Nil(t, res)
	require.Less(t, time.Since(start), time.Second)
}

func TestReserveQueueEvent_ReturnsContextError(t *testing.T) {
	installTestKafkaProducer(t)
	holdAllReservations(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ReserveQueueEvent(ctx, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReserveQueueEvent_SucceedsWhenQueueDrains(t *testing.T) {
	producer, _ := installTestKafkaProducer(t)
	held := holdAllReservations(t)

	go func() {
		time.Sleep(50 * time.Millisecond)
		held[0].Release()
	}()

	res, err := ReserveQueueEvent(context.Background(), 5*time.Second)
	require.NoError(t, err)

	res.Send("evt", map[string]int{"n": 1})
	require.Equal(t, 1, producer.QueueLength())

	// A used reservation is inert.
	res.Send("evt", nil)
	res.Release()
	require.Equal(t, 1, producer.QueueLength())
}

func TestReserveQueueEvent_ReleaseFreesSlot(t *testing.T) {
	installTestKafkaProducer(t)
	held := holdAllReservations(t)

	_, err := ReserveQueueEvent(context.Background(), 10*time.Millisecond)
	require.ErrorIs(t, err, ErrEventQueueFull)

	held[1].Release()
	res, err := ReserveQueueEvent(context.Background(), 10*time.Millisecond)
	require.NoError(t, err)
	res.Release()
}

func TestKafkaProducer_RunDrainsQueueAndReportsLength(t *testing.T) {
	ensureTestCensus(t)

	producer, writer := installTestKafkaProducer(t)

	// Fill: three reserved and two async events, nothing consuming yet.
	for i := 0; i < 3; i++ {
		res, err := ReserveQueueEvent(context.Background(), time.Second)
		require.NoError(t, err)
		res.Send("reserved", i)
	}
	require.NoError(t, SendQueueEventAsync("async", 1))
	require.NoError(t, SendQueueEventAsync("async", 2))
	require.Equal(t, 5, producer.QueueLength())
	require.EqualValues(t, 5, lastGaugeValue(t, "remote_signer_event_queue_length"))

	// Drain.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go producer.Run(ctx)

	require.Eventually(t, func() bool { return writer.count() == 5 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 0, producer.QueueLength())
	require.EqualValues(t, 0, lastGaugeValue(t, "remote_signer_event_queue_length"))

	// Every reserved slot was handed back once its event was consumed.
	for i := 0; i < KafkaChannelSize; i++ {
		res, err := ReserveQueueEvent(context.Background(), 10*time.Millisecond)
		require.NoError(t, err)
		defer res.Release()
	}
}

func TestRemoteSignerEventDropped_Counts(t *testing.T) {
	ensureTestCensus(t)

	before := droppedCount(t, "create_signed_ticket")
	RemoteSignerEventDropped("create_signed_ticket")
	require.Equal(t, before+1, droppedCount(t, "create_signed_ticket"))
}

func droppedCount(t *testing.T, eventType string) int64 {
	t.Helper()
	rows, err := view.RetrieveData("remote_signer_events_dropped_total")
	require.NoError(t, err)
	for _, row := range rows {
		for _, tg := range row.Tags {
			if tg.Key.Name() == "event_type" && tg.Value == eventType {
				return row.Data.(*view.CountData).Value
			}
		}
	}
	return 0
}
