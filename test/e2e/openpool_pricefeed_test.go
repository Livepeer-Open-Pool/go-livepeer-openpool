package e2e

import (
	"encoding/binary"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/core/vm/runtime"
	"github.com/stretchr/testify/require"
)

// The devnet has no Chainlink price feed, and an orchestrator that prices
// apps in US dollars needs one. mockPriceFeed is the smallest contract that
// answers what go-livepeer asks of a feed: description(), decimals() and
// latestRoundData(), with a fixed price and the current block time.
//
// It is written in EVM opcodes because the test has no Solidity compiler.

const (
	opStop         = 0x00
	opEq           = 0x14
	opShr          = 0x1c
	opCalldataload = 0x35
	opCodecopy     = 0x39
	opTimestamp    = 0x42
	opMstore       = 0x52
	opJumpi        = 0x57
	opJumpdest     = 0x5b
	opPush1        = 0x60
	opPush2        = 0x61
	opPush4        = 0x63
	opPush32       = 0x7f
	opDup1         = 0x80
	opReturn       = 0xf3
	opRevert       = 0xfd
)

// asm builds bytecode, with jump targets filled in once they are known.
type asm struct {
	code   []byte
	labels map[string]int
	fixups map[int]string
}

func newAsm() *asm { return &asm{labels: map[string]int{}, fixups: map[int]string{}} }

func (a *asm) op(ops ...byte) *asm { a.code = append(a.code, ops...); return a }

func (a *asm) push1(v byte) *asm { return a.op(opPush1, v) }

func (a *asm) push4(v uint32) *asm {
	a.op(opPush4)
	a.code = binary.BigEndian.AppendUint32(a.code, v)
	return a
}

func (a *asm) push32(v []byte) *asm {
	word := make([]byte, 32)
	copy(word, v)
	return a.op(opPush32).op(word...)
}

// pushUint pushes a number as a full word.
func (a *asm) pushUint(v *big.Int) *asm { return a.op(opPush32).op(v.FillBytes(make([]byte, 32))...) }

// jumpiTo jumps to label when the top of the stack is true.
func (a *asm) jumpiTo(label string) *asm {
	a.op(opPush2)
	a.fixups[len(a.code)] = label
	return a.op(0, 0, opJumpi)
}

func (a *asm) label(name string) *asm {
	a.labels[name] = len(a.code)
	return a.op(opJumpdest)
}

// store writes the word on the stack to memory at offset.
func (a *asm) store(offset byte) *asm { return a.push1(offset).op(opMstore) }

// ret returns size bytes of memory from 0.
func (a *asm) ret(size byte) *asm { return a.push1(size).push1(0).op(opReturn) }

func (a *asm) bytes() []byte {
	out := append([]byte(nil), a.code...)
	for at, label := range a.fixups {
		binary.BigEndian.PutUint16(out[at:], uint16(a.labels[label]))
	}
	return out
}

// mockPriceFeedRuntime is the code of a price feed "ETH / USD" that always
// says one ETH is usdPerEth dollars.
func mockPriceFeedRuntime(usdPerEth int64) []byte {
	const decimals = 8
	answer := new(big.Int).Mul(big.NewInt(usdPerEth), new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil))
	a := newAsm()
	// The first four bytes of the call say which function is asked for.
	a.push1(0).op(opCalldataload).push1(0xe0).op(opShr)
	a.op(opDup1).push4(0x313ce567).op(opEq).jumpiTo("decimals")
	a.op(opDup1).push4(0x7284e416).op(opEq).jumpiTo("description")
	a.op(opDup1).push4(0xfeaf968c).op(opEq).jumpiTo("latestRoundData")
	a.push1(0).push1(0).op(opRevert)

	a.label("decimals").push1(decimals).store(0).ret(0x20)

	// A string is returned as: where it starts, how long it is, its bytes.
	const description = "ETH / USD"
	a.label("description").push1(0x20).store(0).push1(byte(len(description))).store(0x20).push32([]byte(description)).store(0x40).ret(0x60)

	// roundId, answer, startedAt, updatedAt, answeredInRound.
	a.label("latestRoundData").push1(1).store(0).pushUint(answer).store(0x20).
		op(opTimestamp).store(0x40).op(opTimestamp).store(0x60).push1(1).store(0x80).ret(0xa0)
	return a.bytes()
}

// mockPriceFeedCreation is the code that, sent as a transaction with no
// recipient, leaves mockPriceFeedRuntime on the chain.
func mockPriceFeedCreation(usdPerEth int64) []byte {
	code := mockPriceFeedRuntime(usdPerEth)
	const prologue = 12 // the length of what follows, before the runtime code
	init := []byte{opPush2, byte(len(code) >> 8), byte(len(code)), opDup1, opPush1, prologue, opPush1, 0, opCodecopy, opPush1, 0, opReturn}
	if len(init) != prologue {
		panic("mock price feed: the prologue's length is not what it says")
	}
	return append(init, code...)
}

const aggregatorABI = `[
	{"name":"decimals","type":"function","stateMutability":"view","inputs":[],"outputs":[{"type":"uint8"}]},
	{"name":"description","type":"function","stateMutability":"view","inputs":[],"outputs":[{"type":"string"}]},
	{"name":"latestRoundData","type":"function","stateMutability":"view","inputs":[],"outputs":[
		{"name":"roundId","type":"uint80"},{"name":"answer","type":"int256"},{"name":"startedAt","type":"uint256"},
		{"name":"updatedAt","type":"uint256"},{"name":"answeredInRound","type":"uint80"}]}
]`

// TestMockPriceFeedAnswersLikeAChainlinkFeed runs the hand-written contract
// in go-ethereum's EVM and decodes its answers with the feed's own ABI.
func TestMockPriceFeedAnswersLikeAChainlinkFeed(t *testing.T) {
	feed, err := abi.JSON(strings.NewReader(aggregatorABI))
	require.NoError(t, err)

	// Deploy it as a transaction would, then call what was left on the chain.
	cfg := &runtime.Config{Time: 1_800_000_000}
	deployed, address, _, err := runtime.Create(mockPriceFeedCreation(3000), cfg)
	require.NoError(t, err)
	require.Equal(t, mockPriceFeedRuntime(3000), deployed)
	call := func(method string) []interface{} {
		input, err := feed.Pack(method)
		require.NoError(t, err)
		output, _, err := runtime.Call(address, input, cfg)
		require.NoError(t, err, method)
		values, err := feed.Unpack(method, output)
		require.NoError(t, err, method)
		return values
	}

	require.Equal(t, uint8(8), call("decimals")[0])
	require.Equal(t, "ETH / USD", call("description")[0])
	round := call("latestRoundData")
	require.Equal(t, big.NewInt(1), round[0])
	require.Equal(t, big.NewInt(3000_00000000), round[1], "3000 dollars with eight decimals")
	require.Equal(t, big.NewInt(1_800_000_000), round[3], "updated now, so the price is never stale")

	// Anything else it is asked is refused.
	_, _, err = runtime.Call(address, []byte{0xde, 0xad, 0xbe, 0xef}, cfg)
	require.Error(t, err)
}
