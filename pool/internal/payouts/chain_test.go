package payouts_test

import (
	"context"
	"errors"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const testChainID = 1337

// fakeChain is an Ethereum node in memory, enough of one for plain ETH
// transfers: accounts with balances and nonces, a pool of pending
// transactions, and blocks mined when the test says so. It checks
// signatures, nonces and funds the way a node does and answers with a
// node's error texts, because the payout code reads them.
type fakeChain struct {
	mu       sync.Mutex
	balances map[ethcommon.Address]*big.Int
	nonces   map[ethcommon.Address]uint64
	pending  []*types.Transaction
	receipts map[ethcommon.Hash]*types.Receipt
	// refusers are addresses that reject ETH sent to them, as a contract without a payable fallback does.
	refusers map[ethcommon.Address]bool
	signer   types.Signer
}

func newFakeChain() *fakeChain {
	return &fakeChain{
		balances: map[ethcommon.Address]*big.Int{}, nonces: map[ethcommon.Address]uint64{},
		receipts: map[ethcommon.Hash]*types.Receipt{}, refusers: map[ethcommon.Address]bool{},
		signer: types.LatestSignerForChainID(big.NewInt(testChainID)),
	}
}

func (c *fakeChain) balance(addr ethcommon.Address) *big.Int {
	if b, ok := c.balances[addr]; ok {
		return b
	}
	return new(big.Int)
}

// credit adds funds to an account, as a transfer from elsewhere would.
func (c *fakeChain) credit(addr ethcommon.Address, amount *big.Int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.balances[addr] = new(big.Int).Add(c.balance(addr), amount)
}

func (c *fakeChain) ChainID(ctx context.Context) (*big.Int, error) {
	return big.NewInt(testChainID), nil
}

func (c *fakeChain) BalanceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (*big.Int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return new(big.Int).Set(c.balance(account)), nil
}

func (c *fakeChain) NonceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nonces[account], nil
}

func (c *fakeChain) PendingNonceAt(ctx context.Context, account ethcommon.Address) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	nonce := c.nonces[account]
	for _, tx := range c.pending {
		if from, _ := types.Sender(c.signer, tx); from == account && tx.Nonce() >= nonce {
			nonce = tx.Nonce() + 1
		}
	}
	return nonce, nil
}

func (c *fakeChain) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return big.NewInt(100_000_000), nil // 0.1 gwei, about what Arbitrum charges
}

func (c *fakeChain) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if msg.To != nil && c.refusers[*msg.To] {
		return 0, errors.New("execution reverted")
	}
	if msg.Value != nil && c.balance(msg.From).Cmp(msg.Value) < 0 {
		return 0, errors.New("insufficient funds for transfer")
	}
	return 21000, nil
}

func (c *fakeChain) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	from, err := types.Sender(c.signer, tx)
	if err != nil {
		return err // wrong chain id or a bad signature
	}
	if _, mined := c.receipts[tx.Hash()]; mined {
		return errors.New("already known")
	}
	for _, p := range c.pending {
		if p.Hash() == tx.Hash() {
			return errors.New("already known")
		}
	}
	if tx.Nonce() < c.nonces[from] {
		return errors.New("nonce too low")
	}
	c.pending = append(c.pending, tx)
	return nil
}

func (c *fakeChain) TransactionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if receipt, ok := c.receipts[hash]; ok {
		return receipt, nil
	}
	return nil, ethereum.NotFound
}

// Commit mines a block: every pending transaction whose nonce is its
// sender's next and whose sender can pay for it.
func (c *fakeChain) Commit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for progress := true; progress; {
		progress = false
		for i, tx := range c.pending {
			from, _ := types.Sender(c.signer, tx)
			gasCost := new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas()))
			if tx.Nonce() != c.nonces[from] || c.balance(from).Cmp(new(big.Int).Add(tx.Value(), gasCost)) < 0 {
				continue
			}
			c.balances[from] = new(big.Int).Sub(c.balance(from), gasCost)
			receipt := &types.Receipt{TxHash: tx.Hash(), Status: types.ReceiptStatusFailed}
			if tx.To() != nil && !c.refusers[*tx.To()] {
				c.balances[from] = new(big.Int).Sub(c.balance(from), tx.Value())
				c.balances[*tx.To()] = new(big.Int).Add(c.balance(*tx.To()), tx.Value())
				receipt.Status = types.ReceiptStatusSuccessful
			}
			c.nonces[from]++
			c.receipts[tx.Hash()] = receipt
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			progress = true
			break
		}
	}
}

// flakyChain is a chain whose sends can be made to fail, as a process that
// dies around a send would experience it.
type flakyChain struct {
	*fakeChain
	failSends    int // fail this many sends without sending
	sendThenFail int // send, then report failure, this many times
}

func (c *flakyChain) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	if c.failSends > 0 {
		c.failSends--
		return errors.New("connection reset")
	}
	if c.sendThenFail > 0 {
		c.sendThenFail--
		if err := c.fakeChain.SendTransaction(ctx, tx); err != nil {
			return err
		}
		return errors.New("connection reset")
	}
	return c.fakeChain.SendTransaction(ctx, tx)
}
