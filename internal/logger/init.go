// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package logger

import (
	logging "github.com/ipfs/go-log/v2"
)

// Setup configures the default log level for all subsystems and optionally
// attaches labels to every emitted log entry. Call this once at program
// startup before any meaningful logging occurs.
//
// It preserves the format, output paths, and per-subsystem level overrides
// that go-log's package init() already parsed from GOLOG_* environment
// variables (GOLOG_LOG_FMT, GOLOG_LOG_LEVEL, ...), and layers an INFO
// baseline + the caller-supplied labels on top. A nil labels map is allowed.
//
// Labels are visible in JSON output (GOLOG_LOG_FMT=json) and are the
// mechanism by which a process tags every line with identifiers that
// downstream log pipelines (Loki, Alloy, ...) can filter on without
// per-line parsing.
//
// Supported GOLOG_LOG_LEVEL formats, parsed by go-log's init:
//
//	GOLOG_LOG_LEVEL=debug                        — all subsystems at debug
//	GOLOG_LOG_LEVEL=router=debug,policy=debug    — per-subsystem overrides
func Setup(labels map[string]string) {
	cfg := logging.GetConfig()
	cfg.Level = logging.LevelInfo

	if len(labels) > 0 && cfg.Labels == nil {
		cfg.Labels = make(map[string]string, len(labels))
	}
	for k, v := range labels {
		cfg.Labels[k] = v
	}

	logging.SetupLogging(cfg)
}
