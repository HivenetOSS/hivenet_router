// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
	_ "time/tzdata" // embed IANA timezone database so time.LoadLocation works in Docker

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// cetLocation is loaded once at startup. Falls back to UTC if the zone is somehow absent.
var cetLocation = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		return time.UTC
	}
	return loc
}()

// Audit context keys — package-level constants accessible from handlers.go.
const (
	auditKeyModel        = "audit_model"
	auditKeyInputTokens  = "audit_input_tokens"
	auditKeyOutputTokens = "audit_output_tokens"
	auditKeyAgentID      = "audit_agent_id"
	// auditKeyErrorCode carries the precise domain error code (e.g. "queue_full",
	// "no_agents_available"). When set by a handler it takes precedence over
	// statusToErrorCode(), which cannot distinguish multiple domain codes that share
	// the same HTTP status (e.g. five distinct codes all map to 503).
	auditKeyErrorCode = "audit_error_code"
)

// auditLog writes flat JSON audit lines to a dedicated file
// (HIVENET_ROUTER_AUDIT_LOG_PATH, default /var/log/hivenet-router/audit.jsonl).
// Keeping audit output separate from stdout ensures docker compose logs router
// stays human-readable; Promtail scrapes the file via a shared volume.
// Falls back to stdout if the file cannot be opened (e.g. local dev without a volume).
var auditLog = func() *zap.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = ""    // ts is written explicitly as the request start time
	cfg.LevelKey = ""   // level is written as a computed field per request, not a fixed "info"
	cfg.MessageKey = "" // suppress "msg":"" — audit lines carry no message string
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		openAuditFile(),
		zapcore.InfoLevel,
	)
	return zap.New(core).With(zap.String("log_type", "audit"))
}()

// auditLogFile is the underlying *os.File for auditLog (nil when falling back to stdout).
// Retained so SyncAuditLog can flush pending kernel buffers and release the file descriptor.
var auditLogFile *os.File

// openAuditFile opens (or creates) the audit log file for append.
// Path is read from HIVENET_ROUTER_AUDIT_LOG_PATH; defaults to /var/log/hivenet-router/audit.jsonl.
// Falls back to stdout on any error so the server always starts.
func openAuditFile() zapcore.WriteSyncer {
	path := os.Getenv("HIVENET_ROUTER_AUDIT_LOG_PATH")
	if path == "" {
		path = "/var/log/hivenet-router/audit.jsonl"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "audit: cannot create log dir: %v — falling back to stdout\n", err)
		return zapcore.AddSync(os.Stdout)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit: cannot open %s: %v — falling back to stdout\n", path, err)
		return zapcore.AddSync(os.Stdout)
	}
	auditLogFile = f
	// zapcore.Lock wraps the file with a mutex so concurrent goroutines don't interleave lines.
	return zapcore.Lock(zapcore.AddSync(f))
}

// SyncAuditLog flushes any pending audit log entries and closes the underlying file.
// Call this during graceful shutdown — after the HTTP server stops accepting requests
// and before os.Exit — to ensure the last audit lines are written to disk and the
// file descriptor is released cleanly.
// Safe to call multiple times; a nil file is a no-op.
func SyncAuditLog() {
	_ = auditLog.Sync()
	if auditLogFile != nil {
		_ = auditLogFile.Close()
	}
}

// AuditMiddleware emits one JSON audit line per HTTP request after the response is sent.
// It skips health probes (/health) as they are high-frequency with no audit value.
// The audit log is written in a defer block to ensure it runs even if the handler panics.
//
// Placement: Must be after RequestIDMiddleware (so request_id is available)
// and before RecoveryMiddleware (so panics are caught before the post-hook runs).
func AuditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip health probes and CORS preflights — infrastructure noise with no
		// tenant/model/inference value; would generate false error entries (204 != 200).
		if c.Request.URL.Path == "/health" || c.Request.Method == http.MethodOptions {
			c.Next()
			return
		}

		// Defer audit logging to ensure it runs even if handler panics.
		start := time.Now()
		defer func() {
			latencyMs := time.Since(start).Milliseconds()
			status := c.Writer.Status()

			reqID, _ := c.Get("request_id")
			tenantID, _ := c.Get("tenant_id")
			keyID, _ := c.Get("key_id")
			model, _ := c.Get(auditKeyModel)
			inTok, _ := c.Get(auditKeyInputTokens)
			outTok, _ := c.Get(auditKeyOutputTokens)
			agentID, _ := c.Get(auditKeyAgentID)

			// 4xx/5xx → error; 2xx/3xx → success (standard HTTP classification).
			// Using status >= 400 rather than != 200 avoids false errors for valid
			// non-200 success codes (e.g. 204) that could appear on non-inference paths.
			errorCode := ""
			if status >= http.StatusBadRequest {
				if ec, _ := c.Get(auditKeyErrorCode); ec != nil {
					errorCode = strDefault(ec, statusToErrorCode(status))
				} else {
					errorCode = statusToErrorCode(status)
				}
			}

			// Grafana uses this field for color-coding instead of keyword-scanning the JSON.
			level := "info"
			if status >= http.StatusBadRequest {
				level = "error"
			}

			// Extract trace ID from the OTEL span context so audit lines can be
			// correlated with Tempo traces in Grafana dashboards.
			traceID := ""
			if sc := trace.SpanFromContext(c.Request.Context()).SpanContext(); sc.HasTraceID() {
				traceID = sc.TraceID().String()
			}

			auditLog.Info("",
				zap.String("level", level),
				zap.String("trace_id", traceID),
				zap.String("request_id", strDefault(reqID, "")),
				zap.String("tenant_id", strDefault(tenantID, "")),
				zap.String("key_id", strDefault(keyID, "")),
				zap.String("model", strDefault(model, "")),
				zap.Int("status_code", status),
				zap.Int64("latency_ms", latencyMs),
				zap.Int64("input_tokens", int64Default(inTok, 0)),
				zap.Int64("output_tokens", int64Default(outTok, 0)),
				zap.String("agent_id", strDefault(agentID, "")),
				zap.String("error_code", errorCode),
				zap.String("source_ip", c.ClientIP()),
				zap.Time("ts", start.In(cetLocation)),
			)
		}()

		c.Next()
	}
}

// strDefault returns the string value if present and non-empty, otherwise the default.
func strDefault(v interface{}, def string) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return def
}

// int64Default returns the int64 value if present, otherwise the default.
func int64Default(v interface{}, def int64) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	default:
		return def
	}
}

// statusToErrorCode maps HTTP status codes to human-readable error codes.
// Both RPM and token-budget rejections return HTTP 429; the audit log uses
// a single "rate_limit_exceeded" code to keep cardinality low. The distinction
// is visible in Prometheus counters (hivenet_tenant_rate_limited_total vs hivenet_tenant_token_limited_total).
func statusToErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "request_invalid"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "model_forbidden"
	case http.StatusNotFound:
		return "model_not_found"
	case http.StatusTooManyRequests:
		return "rate_limit_exceeded"
	case http.StatusServiceUnavailable:
		return "no_agents_available"
	case http.StatusGatewayTimeout:
		return "request_timeout"
	default:
		return "backend_error"
	}
}
