package payouts

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

// Paying must never pay twice. Each payment gets exactly one signed
// transaction, and that transaction is written to the database before it is
// broadcast. After a crash the service finds the stored transaction and
// either sees it confirmed or sends that same transaction again; it never
// signs a second one for a payment whose first could still be mined.

type sendItem struct {
	memberID, address, amount, status, txHash, rawTx string
	nonce                                            int64
}

// sendApproved makes the payments of approved batches, oldest window first.
func (s *Service) sendApproved(ctx context.Context) error {
	if s.wallet == nil {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM batches WHERE status IN (?, ?, ?) ORDER BY first_round`, StatusApproved, StatusSending, StatusBlocked)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(ids) == 0 {
		return err
	}
	if err := s.wallet.checkChain(ctx); err != nil {
		return err
	}
	for _, id := range ids {
		done, err := s.sendBatch(ctx, id)
		if err != nil {
			return err
		}
		if !done {
			// Payments go out in order, one window at a time: a later batch
			// waits for this one.
			return nil
		}
	}
	return nil
}

func (s *Service) setBatch(ctx context.Context, id, status, note string) error {
	sent := int64(0)
	if status == StatusSent {
		sent = s.nowMs()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE batches SET status = ?, note = ?, sent_at = ? WHERE id = ?`, status, note, sent, id)
	return err
}

func (s *Service) setItem(ctx context.Context, batchID, memberID, status, problem string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE batch_items SET status = ?, error = ?, updated_at = ? WHERE batch_id = ? AND member_id = ?`,
		status, problem, s.nowMs(), batchID, memberID)
	return err
}

// sendBatch moves a batch's payments forward and reports whether the batch
// is finished: every included payment confirmed.
func (s *Service) sendBatch(ctx context.Context, id string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT member_id, address, amount_wei, status, tx_hash, raw_tx, nonce FROM batch_items
		WHERE batch_id = ? AND include = 1 ORDER BY member_id`, id)
	if err != nil {
		return false, err
	}
	var items []sendItem
	for rows.Next() {
		var it sendItem
		if err := rows.Scan(&it.memberID, &it.address, &it.amount, &it.status, &it.txHash, &it.rawTx, &it.nonce); err != nil {
			rows.Close()
			return false, err
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	if err := s.setBatch(ctx, id, StatusSending, ""); err != nil {
		return false, err
	}

	failed := 0
	for _, it := range items {
		status := it.status
		switch status {
		case ItemConfirmed:
			continue
		case ItemFailed:
			failed++
			continue
		case ItemPending:
			var blocked string
			status, blocked, err = s.sign(ctx, id, &it)
			if err != nil {
				return false, err
			}
			if blocked != "" {
				return false, s.setBatch(ctx, id, StatusBlocked, blocked)
			}
			if status == ItemFailed {
				failed++
				continue
			}
		}
		// The payment has a stored transaction: see it through.
		status, err = s.settle(ctx, id, it)
		if err != nil {
			return false, err
		}
		switch status {
		case ItemConfirmed:
		case ItemFailed:
			failed++
		default:
			return false, nil // not confirmed yet; the next pass picks it up
		}
	}
	if failed > 0 {
		return false, s.setBatch(ctx, id, StatusFailed, fmt.Sprintf("%d payment(s) failed", failed))
	}
	s.log.Info("payout batch sent", "batch", id, "payments", len(items))
	return true, s.setBatch(ctx, id, StatusSent, "")
}

// sign builds, signs and stores the one transaction for a pending payment.
// It returns the payment's new status, or why the batch is blocked.
func (s *Service) sign(ctx context.Context, batchID string, it *sendItem) (status, blocked string, err error) {
	w := s.wallet
	amount, ok := new(big.Int).SetString(it.amount, 10)
	if !ok {
		return "", "", fmt.Errorf("stored payment amount %q is not a number", it.amount)
	}
	to := ethcommon.HexToAddress(it.address)
	gasPrice, err := w.chain.SuggestGasPrice(ctx)
	if err != nil {
		return "", "", fmt.Errorf("Ethereum node: %w", err)
	}
	// Headroom, so a small rise in price between signing and mining does not strand the payment.
	gasPrice = new(big.Int).Div(new(big.Int).Mul(gasPrice, big.NewInt(125)), big.NewInt(100))
	gasLimit, err := w.chain.EstimateGas(ctx, ethereum.CallMsg{From: w.address, To: &to, Value: amount})
	if err != nil {
		if isInsufficientFunds(err) {
			return "", s.shortfall(ctx, batchID), nil
		}
		// The recipient cannot take the payment, for instance a contract that rejects it.
		return ItemFailed, "", s.setItem(ctx, batchID, it.memberID, ItemFailed, "the payment cannot be made: "+err.Error())
	}
	balance, err := w.chain.BalanceAt(ctx, w.address, nil)
	if err != nil {
		return "", "", fmt.Errorf("Ethereum node: %w", err)
	}
	cost := new(big.Int).Add(amount, new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasLimit)))
	if balance.Cmp(cost) < 0 {
		return "", s.shortfall(ctx, batchID), nil
	}
	nonce, err := w.chain.PendingNonceAt(ctx, w.address)
	if err != nil {
		return "", "", fmt.Errorf("Ethereum node: %w", err)
	}
	tx, err := types.SignTx(types.NewTx(&types.LegacyTx{Nonce: nonce, To: &to, Value: amount, Gas: gasLimit, GasPrice: gasPrice}),
		types.LatestSignerForChainID(w.chainID), w.key)
	if err != nil {
		return "", "", err
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		return "", "", err
	}
	// Stored before it is broadcast: from here on this is the payment's transaction.
	res, err := s.db.ExecContext(ctx, `UPDATE batch_items SET status = ?, nonce = ?, tx_hash = ?, raw_tx = ?, error = '', updated_at = ? WHERE batch_id = ? AND member_id = ? AND status = ?`,
		ItemSigned, int64(nonce), tx.Hash().Hex(), hexutil.Encode(raw), s.nowMs(), batchID, it.memberID, ItemPending)
	if err != nil {
		return "", "", err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return "", "", errors.New("payment changed while it was being signed")
	}
	it.status, it.nonce, it.txHash, it.rawTx = ItemSigned, int64(nonce), tx.Hash().Hex(), hexutil.Encode(raw)
	return ItemSigned, "", nil
}

func isInsufficientFunds(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "insufficient funds")
}

// shortfall says how much the wallet lacks for what the batch still has to pay.
func (s *Service) shortfall(ctx context.Context, batchID string) string {
	rows, err := s.db.QueryContext(ctx, `SELECT amount_wei FROM batch_items WHERE batch_id = ? AND include = 1 AND status = ?`, batchID, ItemPending)
	if err != nil {
		return "the payout wallet cannot cover the batch"
	}
	defer rows.Close()
	needed := new(big.Int)
	for rows.Next() {
		var amount string
		if rows.Scan(&amount) == nil {
			if n, ok := new(big.Int).SetString(amount, 10); ok {
				needed.Add(needed, n)
			}
		}
	}
	balance, err := s.wallet.chain.BalanceAt(ctx, s.wallet.address, nil)
	if err != nil {
		return "the payout wallet cannot cover the batch"
	}
	missing := new(big.Int).Sub(needed, balance)
	if missing.Sign() <= 0 {
		return fmt.Sprintf("the payout wallet %s cannot cover the gas for the payments left; add funds", s.wallet.address.Hex())
	}
	return fmt.Sprintf("the payout wallet %s holds %s wei and the payments left need %s wei plus gas; add at least %s wei",
		s.wallet.address.Hex(), balance, needed, missing)
}

// settle sees a payment's stored transaction through: confirmed if it was
// mined, sent again if the network does not know it, failed if it can never
// be mined. It returns the payment's status after this pass.
func (s *Service) settle(ctx context.Context, batchID string, it sendItem) (string, error) {
	w := s.wallet
	hash := ethcommon.HexToHash(it.txHash)
	status, err := s.receipt(ctx, batchID, it, hash)
	if err != nil || status != "" {
		return status, err
	}

	// Not mined. If the wallet's confirmed nonce has passed this
	// transaction's, another transaction took its place and it never will be.
	confirmed, err := w.chain.NonceAt(ctx, w.address, nil)
	if err != nil {
		return "", fmt.Errorf("Ethereum node: %w", err)
	}
	if int64(confirmed) > it.nonce {
		// Look once more: it may have been mined between the two questions.
		status, err := s.receipt(ctx, batchID, it, hash)
		if err != nil || status != "" {
			return status, err
		}
		return ItemFailed, s.setItem(ctx, batchID, it.memberID, ItemFailed, "its transaction was replaced by another from the payout wallet; the payment was not made")
	}

	raw, err := hexutil.Decode(it.rawTx)
	if err != nil {
		return "", fmt.Errorf("stored transaction: %w", err)
	}
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(raw); err != nil {
		return "", fmt.Errorf("stored transaction: %w", err)
	}
	if err := w.chain.SendTransaction(ctx, tx); err != nil && !alreadyKnown(err) {
		s.log.Warn("payment not broadcast; it will be sent again", "batch", batchID, "member_id", it.memberID, "tx_hash", it.txHash, "err", err)
		return it.status, nil
	}
	if err := s.setItem(ctx, batchID, it.memberID, ItemBroadcast, ""); err != nil {
		return "", err
	}

	// Give it a moment to confirm, so a batch of a few payments finishes in one pass.
	deadline := time.Now().Add(s.cfg.ConfirmWait)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ItemBroadcast, nil
		case <-time.After(250 * time.Millisecond):
		}
		status, err := s.receipt(ctx, batchID, it, hash)
		if err != nil || status != "" {
			return status, err
		}
	}
	return ItemBroadcast, nil
}

// receipt records a mined transaction's outcome. It returns "" when the
// transaction has not been mined.
func (s *Service) receipt(ctx context.Context, batchID string, it sendItem, hash ethcommon.Hash) (string, error) {
	receipt, err := s.wallet.chain.TransactionReceipt(ctx, hash)
	if errors.Is(err, ethereum.NotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("Ethereum node: %w", err)
	}
	if receipt.Status == types.ReceiptStatusSuccessful {
		if err := s.setItem(ctx, batchID, it.memberID, ItemConfirmed, ""); err != nil {
			return "", err
		}
		_, err := s.db.ExecContext(ctx, `INSERT INTO audit (at, actor, action, subject, detail) VALUES (?, 'pool', 'payment.confirmed', ?, ?)`,
			s.nowMs(), batchID, it.memberID+" "+it.amount+" wei "+it.txHash)
		return ItemConfirmed, err
	}
	return ItemFailed, s.setItem(ctx, batchID, it.memberID, ItemFailed, "its transaction was mined and reverted; the payment was not made")
}

func alreadyKnown(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "already known") || strings.Contains(text, "known transaction") || strings.Contains(text, "nonce too low")
}
