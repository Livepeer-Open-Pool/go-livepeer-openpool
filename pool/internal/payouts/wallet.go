package payouts

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Chain is the Ethereum node the payout wallet sends through.
type Chain interface {
	ChainID(ctx context.Context) (*big.Int, error)
	BalanceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (*big.Int, error)
	PendingNonceAt(ctx context.Context, account ethcommon.Address) (uint64, error)
	NonceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (uint64, error)
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	TransactionReceipt(ctx context.Context, txHash ethcommon.Hash) (*types.Receipt, error)
}

// Wallet is the region's payout wallet: a key and the chain it pays on.
type Wallet struct {
	chain   Chain
	key     *ecdsa.PrivateKey
	address ethcommon.Address
	chainID *big.Int
}

// NewWallet returns a payout wallet for key on chain, which must be the
// chain with the given id.
func NewWallet(chain Chain, key *ecdsa.PrivateKey, chainID int64) *Wallet {
	return &Wallet{chain: chain, key: key, address: crypto.PubkeyToAddress(key.PublicKey), chainID: big.NewInt(chainID)}
}

// OpenWallet connects to the Ethereum node at rpcURL and unlocks the
// keystore file with the password.
func OpenWallet(ctx context.Context, rpcURL, keystorePath, password string, chainID int64) (*Wallet, error) {
	keyJSON, err := os.ReadFile(keystorePath)
	if err != nil {
		return nil, fmt.Errorf("payout keystore: %w", err)
	}
	key, err := keystore.DecryptKey(keyJSON, password)
	if err != nil {
		return nil, fmt.Errorf("payout keystore: %w", err)
	}
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("Ethereum node: %w", err)
	}
	wallet := NewWallet(client, key.PrivateKey, chainID)
	if err := wallet.checkChain(ctx); err != nil {
		return nil, err
	}
	return wallet, nil
}

// Address is where the wallet pays from.
func (w *Wallet) Address() ethcommon.Address { return w.address }

// checkChain refuses a node that is on another chain than the one
// configured: a payment signed for one chain must not be sent on another.
func (w *Wallet) checkChain(ctx context.Context) error {
	id, err := w.chain.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("Ethereum node did not report its chain: %w", err)
	}
	if id.Cmp(w.chainID) != 0 {
		return fmt.Errorf("Ethereum node is on chain %s, the pool is configured for chain %s", id, w.chainID)
	}
	return nil
}

// WalletStatus is the payout wallet as the operator sees it.
type WalletStatus struct {
	Configured bool   `json:"configured"`
	Address    string `json:"address,omitempty"`
	ChainID    int64  `json:"chain_id,omitempty"`
	BalanceWei string `json:"balance_wei,omitempty"`
	// OwedWei is what approved batches still have to pay, gas not included.
	OwedWei string `json:"owed_wei"`
	// AwaitingApprovalWei is what batches waiting for approval would pay.
	AwaitingApprovalWei string `json:"awaiting_approval_wei"`
	Error               string `json:"error,omitempty"`
}

// Wallet reports the payout wallet's balance and what it has to cover.
func (s *Service) Wallet(ctx context.Context) (WalletStatus, error) {
	status := WalletStatus{}
	owed, err := s.unpaid(ctx, StatusApproved, StatusSending, StatusBlocked, StatusFailed)
	if err != nil {
		return status, err
	}
	awaiting, err := s.unpaid(ctx, StatusReady)
	if err != nil {
		return status, err
	}
	status.OwedWei, status.AwaitingApprovalWei = owed.String(), awaiting.String()
	if s.wallet == nil {
		return status, nil
	}
	status.Configured, status.Address, status.ChainID = true, s.wallet.address.Hex(), s.wallet.chainID.Int64()
	balance, err := s.wallet.chain.BalanceAt(ctx, s.wallet.address, nil)
	if err != nil {
		status.Error = "the Ethereum node did not answer: " + err.Error()
		return status, nil
	}
	status.BalanceWei = balance.String()
	return status, nil
}

// unpaid sums the included payments not yet confirmed in batches with one of the statuses.
func (s *Service) unpaid(ctx context.Context, statuses ...string) (*big.Int, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(statuses)), ",")
	args := make([]any, 0, len(statuses)+1)
	for _, st := range statuses {
		args = append(args, st)
	}
	args = append(args, ItemConfirmed)
	rows, err := s.db.QueryContext(ctx, `SELECT i.amount_wei FROM batch_items i JOIN batches b ON b.id = i.batch_id
		WHERE b.status IN (`+placeholders+`) AND i.include = 1 AND i.status != ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sum := new(big.Int)
	for rows.Next() {
		var amount string
		if err := rows.Scan(&amount); err != nil {
			return nil, err
		}
		n, ok := new(big.Int).SetString(amount, 10)
		if !ok {
			return nil, errors.New("stored payment amount is not a number")
		}
		sum.Add(sum, n)
	}
	return sum, rows.Err()
}
