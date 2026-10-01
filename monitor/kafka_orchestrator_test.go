package monitor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func installTestOrchestratorProducer(t *testing.T, enqueueTimeout time.Duration) (*OrchestratorKafkaProducer, *fakeKafkaWriter) {
	t.Helper()
	writer := &fakeKafkaWriter{}
	producer := NewOrchestratorKafkaProducer(writer, "orchestrator-events", enqueueTimeout)
	SetOrchestratorKafkaProducer(producer)
	t.Cleanup(func() { SetOrchestratorKafkaProducer(nil) })
	return producer, writer
}

func orchestratorEventsDropped(t *testing.T, eventType string) int64 {
	t.Helper()
	rows, err := view.RetrieveData("orchestrator_events_dropped_total")
	require.NoError(t, err)
	for _, row := range rows {
		for _, tag := range row.Tags {
			if tag.Key.Name() == "event_type" && tag.Value == eventType {
				return row.Data.(*view.CountData).Value
			}
		}
	}
	return 0
}

func TestSendOrchestratorEvent_NotConfigured(t *testing.T) {
	SetOrchestratorKafkaProducer(nil)
	require.False(t, OrchestratorEventsEnabled())
	require.ErrorIs(t, SendOrchestratorEvent("evt", "key", nil), ErrKafkaProducerNotConfigured)
}

func TestSendOrchestratorEvent_PublishesAFlatKeyedObject(t *testing.T) {
	producer, writer := installTestOrchestratorProducer(t, 0)
	require.True(t, OrchestratorEventsEnabled())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go producer.Run(ctx)

	fields := map[string]interface{}{"runner_id": "runner_abc", "member_id": "m1", "fee_wei": "42", "ts_ms": int64(1790000012345)}
	require.NoError(t, SendOrchestratorEvent("live_runner_fee_debited", "runner_abc", fields))
	require.NoError(t, SendOrchestratorEvent("live_runner_fee_debited", "runner_abc", map[string]interface{}{"runner_id": "runner_abc"}))
	require.Len(t, fields, 4, "the caller's map is not written to")

	require.Eventually(t, func() bool { return writer.count() == 2 }, 5*time.Second, 10*time.Millisecond)
	writer.mu.Lock()
	msgs := writer.msgs
	writer.mu.Unlock()

	var first, second map[string]interface{}
	require.NoError(t, json.Unmarshal(msgs[0].Value, &first))
	require.NoError(t, json.Unmarshal(msgs[1].Value, &second))
	require.Equal(t, "runner_abc", string(msgs[0].Key), "a runner's events share a key, which keeps them in order")
	require.Equal(t, "live_runner_fee_debited", first["event_type"])
	require.Equal(t, "runner_abc", first["runner_id"])
	require.Equal(t, "m1", first["member_id"])
	require.Equal(t, "42", first["fee_wei"])
	require.EqualValues(t, 1790000012345, first["ts_ms"], "the time the caller observed is kept")
	require.NotZero(t, second["ts_ms"], "and filled in when the caller gave none")
	require.NotEmpty(t, first["event_id"])
	require.NotEqual(t, first["event_id"], second["event_id"])
}

func TestSendOrchestratorEvent_DropsAndCountsWhenQueueFull(t *testing.T) {
	ensureTestCensus(t)
	producer, _ := installTestOrchestratorProducer(t, 0)
	for i := 0; i < OrchestratorKafkaChannelSize; i++ {
		require.NoError(t, SendOrchestratorEvent("fill", "k", nil))
	}
	before := orchestratorEventsDropped(t, "live_runner_fee_debited")

	start := time.Now()
	require.ErrorIs(t, SendOrchestratorEvent("live_runner_fee_debited", "k", nil), ErrEventQueueFull)
	require.Less(t, time.Since(start), time.Second, "without an enqueue timeout a full queue drops at once")
	require.Equal(t, OrchestratorKafkaChannelSize, producer.QueueLength())
	require.Equal(t, before+1, orchestratorEventsDropped(t, "live_runner_fee_debited"))
}

func TestSendOrchestratorEvent_WaitsForRoomWhenDeliveryIsRequired(t *testing.T) {
	producer, writer := installTestOrchestratorProducer(t, 5*time.Second)
	for i := 0; i < OrchestratorKafkaChannelSize; i++ {
		require.NoError(t, SendOrchestratorEvent("fill", "k", nil))
	}

	// The queue starts draining shortly after the send began to wait.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		producer.Run(ctx)
	}()
	require.NoError(t, SendOrchestratorEvent("live_runner_fee_debited", "k", nil))
	require.Eventually(t, func() bool { return writer.count() == OrchestratorKafkaChannelSize+1 }, 5*time.Second, 10*time.Millisecond)
}

func TestSendOrchestratorEvent_GivesUpAfterTheEnqueueTimeout(t *testing.T) {
	installTestOrchestratorProducer(t, 50*time.Millisecond)
	for i := 0; i < OrchestratorKafkaChannelSize; i++ {
		require.NoError(t, SendOrchestratorEvent("fill", "k", nil))
	}
	start := time.Now()
	require.ErrorIs(t, SendOrchestratorEvent("live_runner_fee_debited", "k", nil), ErrEventQueueFull)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	require.Less(t, time.Since(start), 2*time.Second)
}
