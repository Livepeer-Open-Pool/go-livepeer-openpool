package compose

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Signed is what a machine's agent is handed: the Compose file and the
// pool's signature over it, the machine it is for and when it was issued.
type Signed struct {
	MachineID string `json:"machine_id"`
	// IssuedAt is when the pool issued this answer, in Unix seconds.
	IssuedAt int64 `json:"issued_at"`
	// Generation identifies the file: it changes when the file does.
	Generation string `json:"generation"`
	Compose    string `json:"compose"`
	// Signature is the base64 Ed25519 signature over the machine, the time
	// and the file.
	Signature string `json:"signature"`
}

func signedBytes(machineID string, issuedAt int64, file string) []byte {
	return []byte("openpool-compose-v1\n" + machineID + "\n" + strconv.FormatInt(issuedAt, 10) + "\n" + file)
}

// Generation identifies a Compose file by its content.
func Generation(file []byte) string {
	sum := sha256.Sum256(file)
	return hex.EncodeToString(sum[:16])
}

// Sign signs file for the machine.
func Sign(key ed25519.PrivateKey, machineID string, issuedAt time.Time, file []byte) Signed {
	signed := Signed{MachineID: machineID, IssuedAt: issuedAt.Unix(), Generation: Generation(file), Compose: string(file)}
	signed.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, signedBytes(machineID, signed.IssuedAt, signed.Compose)))
	return signed
}

// Verify checks that the pool holding key issued signed, and returns the
// Compose file checked against what an agent accepts.
func Verify(key ed25519.PublicKey, signed Signed) (*File, error) {
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil || !ed25519.Verify(key, signedBytes(signed.MachineID, signed.IssuedAt, signed.Compose), signature) {
		return nil, errors.New("the compose file is not signed by this pool's key")
	}
	if signed.Generation != Generation([]byte(signed.Compose)) {
		return nil, errors.New("the compose file's generation does not match its content")
	}
	return Parse([]byte(signed.Compose))
}

// EncodePublicKey renders a public key the way the agent's --key takes it.
func EncodePublicKey(key ed25519.PublicKey) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

// ParsePublicKey reads a key rendered by EncodePublicKey.
func ParsePublicKey(s string) (ed25519.PublicKey, error) {
	key, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("the pool's key must be the 43 characters shown with the install command")
	}
	return ed25519.PublicKey(key), nil
}

// LoadOrCreateKey reads the signing key kept in the file at path, creating
// the file with a new key when there is none. Agents trust the key they were
// installed with, so losing this file means every member installs again.
func LoadOrCreateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		seed := base64.StdEncoding.EncodeToString(key.Seed()) + "\n"
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, fmt.Errorf("creating the compose signing key: %w", err)
		}
		if _, err := file.WriteString(seed); err != nil {
			file.Close()
			return nil, fmt.Errorf("writing the compose signing key: %w", err)
		}
		return key, file.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("reading the compose signing key: %w", err)
	}
	seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("%s does not hold a compose signing key", path)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}
