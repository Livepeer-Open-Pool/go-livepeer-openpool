package payouts_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/stretchr/testify/require"

	"github.com/Livepeer-Open-Pool/pool/internal/payouts"
)

// chainNode is an Ethereum node that only says which chain it is on.
func chainNode(t *testing.T, chainIDHex string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": chainIDHex})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestOpenWalletUnlocksTheKeystoreAndChecksTheChain(t *testing.T) {
	dir := t.TempDir()
	account, err := keystore.NewKeyStore(dir, keystore.LightScryptN, keystore.LightScryptP).NewAccount("correct horse")
	require.NoError(t, err)
	path := account.URL.Path
	ctx := context.Background()
	arbitrum := chainNode(t, "0xa4b1") // 42161

	wallet, err := payouts.OpenWallet(ctx, arbitrum, path, "correct horse", 42161)
	require.NoError(t, err)
	require.Equal(t, account.Address, wallet.Address())

	_, err = payouts.OpenWallet(ctx, arbitrum, path, "wrong password", 42161)
	require.ErrorContains(t, err, "payout keystore")
	_, err = payouts.OpenWallet(ctx, arbitrum, dir+"/missing", "correct horse", 42161)
	require.ErrorContains(t, err, "payout keystore")

	// A node on another chain than the one configured is refused before anything is signed.
	_, err = payouts.OpenWallet(ctx, chainNode(t, "0x1"), path, "correct horse", 42161)
	require.ErrorContains(t, err, "is on chain 1, the pool is configured for chain 42161")
}
