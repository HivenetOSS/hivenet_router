// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mr-tron/base58"
)

// keyPreview returns a masked display form of a raw key for human identification.
// Format: "sk-..." + last 4 chars of the full key.
// Example: "sk-...KJ4"
func keyPreview(key string) string {
	const tail = 4
	if len(key) <= tail {
		return key
	}
	return "sk-..." + key[len(key)-tail:]
}

// runKeygen implements the "keygen" subcommand.
// It generates a cryptographically random API key, prints it once (plaintext is
// never stored by the router), and prints the YAML snippet to add to auth.yaml.
//
// Usage: hivenet-router keygen [--tenant <name>]
func runKeygen(args []string) {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	tenant := fs.String("tenant", "my-tenant", "Tenant name for the generated key entry")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	const maxTenantLen = 200
	if len(*tenant) > maxTenantLen {
		fmt.Fprintf(os.Stderr, "keygen: --tenant value too long (%d chars, max %d)\n", len(*tenant), maxTenantLen)
		os.Exit(1)
	}

	// Generate 32 cryptographically random bytes.
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		fmt.Fprintf(os.Stderr, "keygen: crypto/rand unavailable: %v\n", err)
		os.Exit(1)
	}

	// Encode as sk-hivenet-<base58> — recognisable prefix, URL-safe, no ambiguous chars.
	key := "sk-hivenet-" + base58.Encode(raw)

	// Compute SHA-256 of the full key string. This hash goes into auth.yaml.
	// The router hashes incoming bearer tokens the same way and compares to this.
	h := sha256.Sum256([]byte(key))
	keyHash := hex.EncodeToString(h[:])

	// Build a masked preview for human identification: "sk-...XXXX".
	// Stored in auth.yaml alongside the hash for operator reference — never used for auth.
	preview := keyPreview(key)

	// Today's date for created_at.
	createdAt := time.Now().UTC().Format("02-01-2006")

	fmt.Println("Key (give this to your client — shown once, never stored):")
	fmt.Println(" ", key)
	fmt.Println()
	fmt.Println("Paste this under `keys:` in your auth.yaml:")
	fmt.Printf("    - key_hash: %q\n", keyHash)
	fmt.Printf("      key_preview: %q\n", preview)
	fmt.Println("      metadata:")
	fmt.Printf("        name: %q\n", *tenant+" key")
	fmt.Printf("        owner: %q\n", *tenant)
	fmt.Println("        description: \"\"")
	fmt.Printf("        created_at: %q\n", createdAt)
	fmt.Println("        # expires_at: \"01-01-2027\"   # optional DD-MM-YYYY")
	fmt.Println("      # models: []   # empty = access to all models")
	fmt.Println("      # models:")
	fmt.Println("      #   - \"meta-llama/Llama-3.1-8B-Instruct\"")
	fmt.Println("      quota:")
	fmt.Println("        requests_per_minute: 100")
	fmt.Println("        tokens_per_day: 500000")
}
