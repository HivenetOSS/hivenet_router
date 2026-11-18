// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ShareValue publishes this run's value under name, for the other routers of
// the site to compare with theirs, and returns, by router ID, the value of
// each router's newest run among those that published within window, this
// router's own run included. A router restarted several times within the
// window counts once, with its current value, and a router is never its own
// peer. Values older than an hour are dropped. It fails at once, without
// waiting, while Valkey is not answering (before the first sync tick
// included).
//
// The router uses it for what every router of a site must agree on but must
// not publish in clear, such as a fingerprint of the cache-salt secret.
func (s *Store) ShareValue(ctx context.Context, name, value string, window time.Duration) (map[string]string, error) {
	c, err := s.connected()
	if err != nil {
		return nil, err
	}
	v, err := shareValue.Exec(ctx, c, []string{s.key("shared", name)},
		[]string{s.inc, value, formatMs(window)}).ToArray()
	if err != nil {
		return nil, err
	}
	type newest struct {
		start int64
		value string
	}
	byRouter := make(map[string]newest, len(v)/2)
	for i := 0; i+1 < len(v); i += 2 {
		run, err1 := v[i].ToString()
		val, err2 := v[i+1].ToString()
		if err1 != nil || err2 != nil {
			continue
		}
		id := routerOfRun(run)
		if run == s.inc {
			// This run is this router's newest, whatever the clocks say.
			byRouter[id] = newest{start: 1<<63 - 1, value: val}
			continue
		}
		if cur, ok := byRouter[id]; !ok || runStart(run) > cur.start {
			byRouter[id] = newest{start: runStart(run), value: val}
		}
	}
	out := make(map[string]string, len(byRouter))
	for id, n := range byRouter {
		out[id] = n.value
	}
	return out, nil
}

// routerOfRun returns the router ID of a run (RouterID@start-ms).
func routerOfRun(run string) string {
	if i := strings.LastIndexByte(run, '@'); i >= 0 {
		return run[:i]
	}
	return run
}

func formatMs(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}

// shareValue sets this run's field (value|now, Valkey's clock) and returns
// run, value for every field set within ARGV[3] ms, dropping fields older
// than an hour.
// KEYS: the shared hash. ARGV: run, value, window (ms).
var shareValue = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. '|' .. now)
redis.call('EXPIRE', KEYS[1], 3600)
local out = {}
local kv = redis.call('HGETALL', KEYS[1])
for i = 1, #kv, 2 do
  local val, ts = string.match(kv[i+1], '^(.*)|(%d+)$')
  if ts then
    local age = now - tonumber(ts)
    if age <= tonumber(ARGV[3]) then
      out[#out + 1] = kv[i]
      out[#out + 1] = val
    elseif age > 3600000 then
      redis.call('HDEL', KEYS[1], kv[i])
    end
  end
end
return out`)
