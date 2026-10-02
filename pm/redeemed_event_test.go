package pm

import (
	"context"
	"encoding/json"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/livepeer/go-livepeer/monitor"
)

type recordingKafkaWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
}

func (w *recordingKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func (w *recordingKafkaWriter) events(t *testing.T) []map[string]interface{} {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	out := []map[string]interface{}{}
	for _, msg := range w.msgs {
		var event map[string]interface{}
		require.NoError(t, json.Unmarshal(msg.Value, &event))
		out = append(out, event)
	}
	return out
}

// recordOrchestratorEvents publishes orchestrator events to a writer the test reads.
func recordOrchestratorEvents(t *testing.T) *recordingKafkaWriter {
	t.Helper()
	writer := &recordingKafkaWriter{}
	producer := monitor.NewOrchestratorKafkaProducer(writer, "orchestrator-events", 0)
	monitor.SetOrchestratorKafkaProducer(producer)
	ctx, cancel := context.WithCancel(context.Background())
	go producer.Run(ctx)
	t.Cleanup(func() {
		cancel()
		monitor.SetOrchestratorKafkaProducer(nil)
	})
	return writer
}

// redeemQueuedTickets queues n tickets from one sender and lets the queue
// hand them to a consumer that answers each redemption with redemptionErr.
func redeemQueuedTickets(t *testing.T, n int, redemptionErr error) []*SignedTicket {
	t.Helper()
	sender := RandAddress()
	sm := &LocalSenderMonitor{
		ticketStore: newStubTicketStore(),
		tm:          &stubTimeManager{round: big.NewInt(100)},
	}
	q := newTicketQueue(sender, sm)
	q.Start()
	t.Cleanup(q.Stop)

	tickets := make([]*SignedTicket, 0, n)
	for i := 0; i < n; i++ {
		ticket := defaultSignedTicket(sender, uint32(i))
		ticket.FaceValue = big.NewInt(int64(1000 * (i + 1)))
		ticket.CreationRound = 99
		require.NoError(t, q.Add(ticket))
		tickets = append(tickets, ticket)
	}
	qc := &queueConsumer{redemptionErr: redemptionErr}
	done := make(chan struct{})
	go qc.Wait(n, q, done)
	time.Sleep(20 * time.Millisecond)
	sm.tm.(*stubTimeManager).blockNumSink <- big.NewInt(1)
	<-done
	require.Eventually(t, func() bool {
		qlen, err := q.Length()
		return err == nil && qlen == 0
	}, 5*time.Second, 5*time.Millisecond)
	return tickets
}

func TestRedeemedTicketIsReportedWithItsValueAndRound(t *testing.T) {
	writer := recordOrchestratorEvents(t)
	tickets := redeemQueuedTickets(t, 3, nil)

	require.Eventually(t, func() bool { return len(writer.events(t)) == 3 }, 5*time.Second, 10*time.Millisecond)
	for i, event := range writer.events(t) {
		ticket := tickets[i]
		require.Equal(t, TicketRedeemedEvent, event["event_type"])
		require.Equal(t, ticket.Recipient.Hex(), event["orch_address"])
		require.Equal(t, ticket.Sender.Hex(), event["sender"])
		require.Equal(t, ticket.FaceValue.String(), event["face_value_wei"])
		require.Equal(t, ticket.Hash().Hex(), event["tx_hash"])
		require.EqualValues(t, 100, event["round"], "the round the ticket was redeemed in")
		require.EqualValues(t, 99, event["creation_round"])
		require.NotEmpty(t, event["event_id"])
		require.NotZero(t, event["ts_ms"])
	}
}

func TestTicketTheNodeGaveUpOnIsNotReported(t *testing.T) {
	writer := recordOrchestratorEvents(t)
	// A used ticket is marked redeemed so the queue moves on; it brought in nothing.
	redeemQueuedTickets(t, 2, errIsUsedTicket)

	time.Sleep(monitor.KafkaBatchInterval + 200*time.Millisecond)
	require.Empty(t, writer.events(t))
}

func TestRedemptionsWorkWithoutOrchestratorEvents(t *testing.T) {
	monitor.SetOrchestratorKafkaProducer(nil)
	redeemQueuedTickets(t, 2, nil)
}
