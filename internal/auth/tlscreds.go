// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"time"

	"golang.org/x/crypto/hkdf"
)

// DeriveGRPCCredentials produces a deterministic ED25519 TLS certificate
// from the JWT secret using HKDF-SHA256. Calling it with the same secret always
// yields the same key pair, allowing the router to authenticate itself to the agent
// over TLS via public key pinning, without any external PKI or pre-distributed
// certificate files.
//
// The router uses the returned tls.Certificate as its gRPC server certificate.
// The agent uses the returned ed25519.PublicKey to pin the router's identity inside
// VerifyPeerCertificate — an on-path attacker cannot forge a matching certificate
// without knowing the JWT secret.
//
// ED25519 key generation (ed25519.NewKeyFromSeed) is deterministic by specification:
// it reads exactly SeedSize bytes from the HKDF and constructs the key without any
// internal randomness, unlike ecdsa.GenerateKey which calls randutil.MaybeReadByte
// on the provided reader (Go 1.17+), making HKDF-derived ECDSA keys non-deterministic.
//
// The salt "hivenet-router-grpc-tls-v2" ensures the derived key material is independent
// from the HMAC-SHA256 key used for JWT signing, and is distinct from the v1 salt
// used by the deprecated ECDSA path — mixed-version deployments fail immediately
// with a deterministic key mismatch rather than intermittently.
func DeriveGRPCCredentials(secret []byte) (tls.Certificate, ed25519.PublicKey, error) {
	hkdfRdr := hkdf.New(sha256.New, secret, []byte("hivenet-router-grpc-tls-v2"), []byte("ed25519"))
	seed := make([]byte, ed25519.SeedSize)
	if _, err := io.ReadFull(hkdfRdr, seed); err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("derive TLS key seed: %w", err)
	}
	privKey := ed25519.NewKeyFromSeed(seed)

	// Self-sign with a far-future validity window. Expiry is irrelevant here
	// because the client pins the public key rather than validating a cert chain.
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hivenet-router-grpc"},
		NotBefore:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(2124, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, privKey.Public(), privKey)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("self-sign TLS cert: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("marshal TLS key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("build TLS cert pair: %w", err)
	}

	pubKey := privKey.Public().(ed25519.PublicKey)
	return tlsCert, pubKey, nil
}
