package tunnelclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTakesACredentialOrTheOrchSecret(t *testing.T) {
	base := Config{Orchestrator: "https://orch.example:8935", Target: "http://127.0.0.1:8000", App: "echo"}

	withSecret := base
	withSecret.OrchSecret = "orch-secret"
	client, err := New(withSecret)
	require.NoError(t, err)
	require.Equal(t, "orch-secret", client.registrationAuth())

	withCredential := base
	withCredential.Credential = " member-credential\n"
	client, err = New(withCredential)
	require.NoError(t, err)
	require.Equal(t, "member-credential", client.registrationAuth())

	_, err = New(base)
	require.EqualError(t, err, "credential or orchSecret is required")

	both := base
	both.OrchSecret, both.Credential = "orch-secret", "member-credential"
	_, err = New(both)
	require.EqualError(t, err, "credential and orchSecret are exclusive")
}
