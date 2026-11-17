package crypto

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
)

// Identity holds a persistent NaCl keypair that identifies this user.
type Identity struct {
	PublicKey  [32]byte `json:"public_key"`
	PrivateKey [32]byte `json:"private_key"`
	Name       string   `json:"name"`
}

// SharedKey is a precomputed symmetric key for peer-to-peer encryption.
// Derived from our private key + peer's public key via Curve25519 ECDH,
// then hashed with a domain separator for use with secretbox.
type SharedKey [32]byte

// =============================================================================
// Identity management
// =============================================================================

// GenerateIdentity creates a fresh NaCl keypair.
func GenerateIdentity(name string) (*Identity, error) {
	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate keypair: %w", err)
	}
	return &Identity{
		PublicKey:  *pub,
		PrivateKey: *priv,
		Name:       name,
	}, nil
}

// LoadIdentity reads an identity from a JSON file, or creates a new one.
func LoadIdentity(path, name string) (*Identity, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		var id Identity
		if err := json.Unmarshal(data, &id); err != nil {
			return nil, fmt.Errorf("parse identity file: %w", err)
		}
		// Always use the saved name — change via UI (click name) or delete identity.json to reset
		return &id, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read identity file: %w", err)
	}

	// Create new identity
	id, err := GenerateIdentity(name)
	if err != nil {
		return nil, err
	}
	if err := id.Save(path); err != nil {
		return nil, err
	}
	return id, nil
}

// Save writes the identity to a JSON file (permissions 0600).
func (id *Identity) Save(path string) error {
	data, err := json.MarshalIndent(id, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal identity: %w", err)
	}
	return os.WriteFile(path, data, 0600)
}

// =============================================================================
// Key exchange
// =============================================================================

// ComputeShared derives the symmetric key for peer communication.
// Uses NaCl box.Precompute (Curve25519 ECDH) to derive the shared secret.
func (id *Identity) ComputeShared(peerPublic [32]byte) SharedKey {
	var shared SharedKey
	box.Precompute((*[32]byte)(&shared), &peerPublic, &id.PrivateKey)
	return shared
}

// =============================================================================
// Encryption / Decryption (secretbox: XSalsa20-Poly1305)
// =============================================================================

// Encrypt encrypts plaintext using the shared key.
// Returns: 24-byte nonce || ciphertext || authentication tag.
func Encrypt(shared SharedKey, plaintext []byte) ([]byte, error) {
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	out := make([]byte, 24)
	copy(out, nonce[:])
	return secretbox.Seal(out, plaintext, &nonce, (*[32]byte)(&shared)), nil
}

// Decrypt decrypts ciphertext produced by Encrypt.
// Expects format: 24-byte nonce || ciphertext.
func Decrypt(shared SharedKey, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 24 {
		return nil, fmt.Errorf("ciphertext too short: %d bytes", len(ciphertext))
	}
	var nonce [24]byte
	copy(nonce[:], ciphertext[:24])
	decrypted, ok := secretbox.Open(nil, ciphertext[24:], &nonce, (*[32]byte)(&shared))
	if !ok {
		return nil, fmt.Errorf("decryption failed: authentication failed")
	}
	return decrypted, nil
}

// =============================================================================
// Fingerprints
// =============================================================================

// Fingerprint returns a formatted fingerprint string.
// Example: "A3F2 901B C744 2E81 D599 6F33 0ABC 1DEF"
func Fingerprint(pub [32]byte) string {
	h := hex.EncodeToString(pub[:])
	return fmt.Sprintf("%s %s %s %s %s %s %s %s",
		h[0:8], h[8:16], h[16:24], h[24:32],
		h[32:40], h[40:48], h[48:56], h[56:64])
}

// ShortFingerprint returns the first 8 hex chars for quick comparison.
func ShortFingerprint(pub [32]byte) string {
	return hex.EncodeToString(pub[:4])
}

// =============================================================================
// Hex helpers
// =============================================================================

// HexEncode encodes bytes as a hex string.
func HexEncode(data []byte) string {
	return hex.EncodeToString(data)
}

// HexDecode decodes a hex string to bytes.
func HexDecode(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

// PubKeyFromHex parses a hex string into a [32]byte public key.
func PubKeyFromHex(s string) ([32]byte, error) {
	var key [32]byte
	data, err := hex.DecodeString(s)
	if err != nil {
		return key, fmt.Errorf("decode hex: %w", err)
	}
	if len(data) != 32 {
		return key, fmt.Errorf("invalid key length: %d, expected 32", len(data))
	}
	copy(key[:], data)
	return key, nil
}
