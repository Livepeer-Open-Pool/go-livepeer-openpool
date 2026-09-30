package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/go-livepeer/pm"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

var initTestCensusOnce sync.Once

// enableTestMetrics initialises census metrics once per test binary and
// turns recording on for the duration of the test.
func enableTestMetrics(t *testing.T) {
	t.Helper()
	initTestCensusOnce.Do(func() {
		monitor.NodeID = "testid"
		monitor.InitCensus(monitor.Broadcaster, "testversion")
	})
	monitor.Enabled = true
	t.Cleanup(func() { monitor.Enabled = false })
}

type discardKafkaWriter struct{}

func (discardKafkaWriter) WriteMessages(context.Context, ...kafka.Message) error { return nil }

// installTestKafkaProducer installs a producer whose queue is never consumed,
// so tests control exactly how full it is.
func installTestKafkaProducer(t *testing.T) *monitor.KafkaProducer {
	t.Helper()
	producer := monitor.NewKafkaProducer(discardKafkaWriter{}, "topic", "gateway")
	monitor.SetKafkaProducer(producer)
	t.Cleanup(func() { monitor.SetKafkaProducer(nil) })
	return producer
}

func holdAllEventReservations(t *testing.T) []*monitor.EventReservation {
	t.Helper()
	held := make([]*monitor.EventReservation, 0, monitor.KafkaChannelSize)
	for i := 0; i < monitor.KafkaChannelSize; i++ {
		res, err := monitor.ReserveQueueEvent(context.Background(), time.Second)
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

func signedTicketDroppedCount(t *testing.T) int64 {
	t.Helper()
	rows, err := view.RetrieveData("remote_signer_events_dropped_total")
	require.NoError(t, err)
	for _, row := range rows {
		for _, tg := range row.Tags {
			if tg.Key.Name() == "event_type" && tg.Value == createSignedTicketEvent {
				return row.Data.(*view.CountData).Value
			}
		}
	}
	return 0
}

type signerTestServer struct {
	ls     *LivepeerServer
	sender *pm.MockSender
	req    []byte
}

func newSignerTestServer(t *testing.T) *signerTestServer {
	t.Helper()
	ethClient := newTestEthClient(t)
	node, _ := core.NewLivepeerNode(ethClient, "", nil)
	node.Balances = core.NewAddressBalances(1 * time.Minute)
	sender := newMockSender(mockSenderConfig{})
	node.Sender = sender

	oInfo := &net.OrchestratorInfo{
		Address:   ethClient.addr.Bytes(),
		PriceInfo: &net.PriceInfo{PricePerUnit: 1, PixelsPerUnit: 1},
		TicketParams: &net.TicketParams{
			Recipient: pm.RandAddress().Bytes(),
		},
		AuthToken: stubAuthToken,
	}
	orchBlob, err := proto.Marshal(oInfo)
	require.NoError(t, err)
	reqBody, err := json.Marshal(RemotePaymentRequest{
		Orchestrator: orchBlob,
		ManifestID:   "manifest",
		InPixels:     1,
	})
	require.NoError(t, err)

	return &signerTestServer{
		ls:     &LivepeerServer{LivepeerNode: node},
		sender: sender,
		req:    reqBody,
	}
}

func (s *signerTestServer) sign() *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/generate-live-payment", bytes.NewReader(s.req))
	rr := httptest.NewRecorder()
	s.ls.GenerateLivePayment(rr, req)
	return rr
}

func TestGenerateLivePayment_EventDropped_SignsAndCounts(t *testing.T) {
	enableTestMetrics(t)
	producer := installTestKafkaProducer(t)
	for i := 0; i < monitor.KafkaChannelSize; i++ {
		require.NoError(t, monitor.SendQueueEventAsync("fill", i))
	}
	before := signedTicketDroppedCount(t)

	s := newSignerTestServer(t)
	rr := s.sign()

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	s.sender.AssertCalled(t, "CreateTicketBatch", mock.Anything, mock.Anything)
	require.Equal(t, before+1, signedTicketDroppedCount(t))
	require.Equal(t, monitor.KafkaChannelSize, producer.QueueLength())
}

func TestGenerateLivePayment_EventQueued_DoesNotCount(t *testing.T) {
	enableTestMetrics(t)
	producer := installTestKafkaProducer(t)
	before := signedTicketDroppedCount(t)

	s := newSignerTestServer(t)
	rr := s.sign()

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Equal(t, before, signedTicketDroppedCount(t))
	// genPayment queues create_new_payment, then create_signed_ticket follows.
	require.Equal(t, 2, producer.QueueLength())
}

func TestGenerateLivePayment_RequireDelivery_RefusesWhenQueueFull(t *testing.T) {
	enableTestMetrics(t)
	producer := installTestKafkaProducer(t)
	holdAllEventReservations(t)

	s := newSignerTestServer(t)
	s.ls.LivepeerNode.KafkaRequireDelivery = true
	s.ls.LivepeerNode.KafkaEnqueueTimeout = 100 * time.Millisecond

	start := time.Now()
	rr := s.sign()

	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.JSONEq(t, `{"error":"event log unavailable"}`, strings.TrimSpace(rr.Body.String()))

	// Nothing was signed and no balance state was advanced.
	s.sender.AssertNotCalled(t, "CreateTicketBatch", mock.Anything, mock.Anything)
	s.sender.AssertNotCalled(t, "Nonce", mock.Anything)
	require.Equal(t, 0, producer.QueueLength())
}

func TestGenerateLivePayment_RequireDelivery_RefusesWithoutProducer(t *testing.T) {
	enableTestMetrics(t)
	monitor.SetKafkaProducer(nil)

	s := newSignerTestServer(t)
	s.ls.LivepeerNode.KafkaRequireDelivery = true
	s.ls.LivepeerNode.KafkaEnqueueTimeout = time.Second

	rr := s.sign()

	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.JSONEq(t, `{"error":"event log unavailable"}`, strings.TrimSpace(rr.Body.String()))
	s.sender.AssertNotCalled(t, "CreateTicketBatch", mock.Anything, mock.Anything)
}

func TestGenerateLivePayment_RequireDelivery_WaitsForDrainingQueue(t *testing.T) {
	enableTestMetrics(t)
	producer := installTestKafkaProducer(t)
	held := holdAllEventReservations(t)

	s := newSignerTestServer(t)
	s.ls.LivepeerNode.KafkaRequireDelivery = true
	s.ls.LivepeerNode.KafkaEnqueueTimeout = 5 * time.Second

	go func() {
		time.Sleep(100 * time.Millisecond)
		held[0].Release()
	}()

	rr := s.sign()

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp RemotePaymentResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.NotEmpty(t, resp.Payment)
	s.sender.AssertCalled(t, "CreateTicketBatch", mock.Anything, mock.Anything)
	// genPayment queues create_new_payment, then create_signed_ticket follows.
	require.Equal(t, 2, producer.QueueLength())

	// The freed slot is now occupied by the queued create_signed_ticket event.
	_, err := monitor.ReserveQueueEvent(context.Background(), 10*time.Millisecond)
	require.ErrorIs(t, err, monitor.ErrEventQueueFull)
}

func TestGenerateLivePayment_RequireDelivery_ReleasesSlotOnSigningError(t *testing.T) {
	enableTestMetrics(t)
	installTestKafkaProducer(t)

	s := newSignerTestServer(t)
	s.ls.LivepeerNode.KafkaRequireDelivery = true
	s.ls.LivepeerNode.KafkaEnqueueTimeout = time.Second
	// Break signing after the reservation is taken.
	sender := &pm.MockSender{}
	sender.On("StartSessionWithNonce", mock.Anything, mock.Anything).Return("pmSession")
	sender.On("CleanupSession", mock.Anything).Maybe()
	sender.On("StopSession", mock.Anything).Maybe()
	sender.On("ValidateTicketParams", mock.Anything).Return(nil)
	sender.On("EV", mock.Anything).Return(big.NewRat(1, 1), nil)
	sender.On("CreateTicketBatch", mock.Anything, mock.Anything).Return(nil, errors.New("signing broken"))
	s.ls.LivepeerNode.Sender = sender

	rr := s.sign()
	require.NotEqual(t, http.StatusOK, rr.Code)

	// Every slot must be free again.
	for i := 0; i < monitor.KafkaChannelSize; i++ {
		res, err := monitor.ReserveQueueEvent(context.Background(), 10*time.Millisecond)
		require.NoError(t, err)
		defer res.Release()
	}
}

func TestSignedTicketDropLogFields(t *testing.T) {
	fields := signedTicketDropLogFields("req", "manifest", "pmSession", "0xabc", "857", 3, 9)
	require.Len(t, fields, 14)
	keys := map[string]interface{}{}
	for i := 0; i < len(fields); i += 2 {
		keys[fields[i].(string)] = fields[i+1]
	}
	require.Equal(t, map[string]interface{}{
		"request_id":      "req",
		"manifest_id":     "manifest",
		"pm_session_id":   "pmSession",
		"orch_address":    "0xabc",
		"computed_fee":    "857",
		"num_tickets":     3,
		"sequence_number": uint64(9),
	}, keys)
}
