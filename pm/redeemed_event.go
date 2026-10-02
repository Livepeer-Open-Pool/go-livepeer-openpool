package pm

import (
	"errors"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/golang/glog"

	"github.com/livepeer/go-livepeer/monitor"
)

// TicketRedeemedEvent is the orchestrator event emitted when this node
// redeems a winning ticket. Its face values, summed over a span of rounds,
// are what the node earned in that span: the amount a pool pays members from,
// known from the node itself and not looked up on-chain.
const TicketRedeemedEvent = "orchestrator_ticket_redeemed"

// reportTicketRedeemed publishes the redemption of ticket in round, the
// current round, not the one the ticket was created in. A redemption the node
// gave up on (a used ticket, a failed transaction) is not reported: it
// brought in nothing.
func reportTicketRedeemed(ticket *SignedTicket, txHash ethcommon.Hash, round *big.Int) {
	fields := map[string]interface{}{
		"orch_address":   ticket.Recipient.Hex(),
		"sender":         ticket.Sender.Hex(),
		"face_value_wei": ticket.FaceValue.String(),
		"tx_hash":        txHash.Hex(),
		"creation_round": ticket.CreationRound,
	}
	if round != nil {
		fields["round"] = round.Int64()
	}
	err := monitor.SendOrchestratorEvent(TicketRedeemedEvent, ticket.Recipient.Hex(), fields)
	if err == nil || errors.Is(err, monitor.ErrKafkaProducerNotConfigured) {
		return
	}
	// The event is part of a pool's accounting record: leave it in the log.
	glog.Errorf("Redeemed ticket not reported err=%q sender=%v face_value_wei=%v tx_hash=%v round=%v creation_round=%v",
		err, ticket.Sender.Hex(), ticket.FaceValue, txHash.Hex(), round, ticket.CreationRound)
}
