# Key Rotation

Procedures for rotating JWT secrets and API keys without service disruption.

## JWT Secret Rotation

JWT secrets authenticate agents to the router. Rotation requires a coordinated restart because all agents share the same secret.

### Rotation Procedure

1. **Generate new secret**

```bash
openssl rand -base64 32 > jwt_secret_new.txt
```

2. **Deploy to all agents** (during maintenance window)

```bash
# On each agent server
cp jwt_secret_new.txt /opt/hivenet-router/jwt.secret
# Restart the agent — it will re-authenticate with the new secret
```

3. **Deploy to router**

```bash
# On router server
cp jwt_secret_new.txt /opt/hivenet-router/jwt.secret
./bin/hivenet-router --jwt-secret-file /opt/hivenet-router/jwt.secret
```

## API Key Rotation

API keys can be rotated individually without restarting the router.

### Rotate a Single Key (Static Mode)

1. **Generate replacement key**

```bash
./bin/hivenet-router keygen --tenant acme-corp
```

Copy the output YAML snippet into `auth.yaml` alongside the existing key.

2. **Update client applications** with the new raw key value.

3. **Remove old key** from `auth.yaml` once all clients are migrated.

4. **Hot-reload**

```bash
kill -HUP $(pgrep hivenet-router)
```

### Add Before Remove

Always add the new key and reload **before** removing the old key. This lets you migrate clients without downtime:

```
Step 1: auth.yaml has [old-key, new-key] → reload
Step 2: migrate clients to new-key
Step 3: auth.yaml has [new-key] → reload
```

### Rotate a Key in Dynamic Mode

In dynamic mode, key rotation is handled entirely by the machines service via the admin API (no SIGHUP involved).

1. **Upsert the new key** (or update the existing one)

```bash
curl -X PUT \
  -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  -H "Content-Type: application/json" \
  -d '{
    "version": "rev_00000002",
    "key_hash": "<new-hash>",
    "owner": "acme-corp",
    "name": "Acme Production Key (rotated)",
    "enabled": true
  }' \
  http://localhost:8080/admin/api-keys/key-abc123
```

2. **Update client applications** with the new raw key.

3. **Revoke the old key** by deleting it (or upserting with `"enabled": false`).

```bash
curl -X DELETE \
  -H "Authorization: Bearer $HIVENET_ROUTER_ADMIN_API_KEYS" \
  "http://localhost:8080/admin/api-keys/key-old123?version=rev_00000003"
```

SIGHUP-based rotation does not apply in dynamic mode — the key registry is managed exclusively through the admin API.

## Rotation Best Practices

### Scheduling

- **JWT secrets:** Quarterly or after security incidents
- **API keys:** On employee departure, suspected compromise, or annual rotation
- **Admin keys:** Same cadence as JWT secrets

### Communication

1. **Notify stakeholders** — Give advance notice (at least 1 week)
2. **Document timeline** — Clear start/end dates
3. **Provide rollback plan** — Keep old keys active initially
4. **Verify completion** — Confirm all clients updated before removing old keys

### Monitoring

Track rotation progress:

```promql
# Requests by tenant (look for drop when old key removed)
sum by (tenant_id) (rate(hivenet_router_routing_requests_routed_total[1h]))
```

## Emergency Rotation

### Compromised API Key

1. **Remove key immediately** — Edit `auth.yaml`, delete the compromised entry.

2. **Hot-reload**

```bash
kill -HUP $(pgrep hivenet-router)
```

3. **Audit impact**

```logql
{job="router"} | json | tenant_id = "affected-tenant"
```

4. **Generate replacement**

```bash
./bin/hivenet-router keygen --tenant affected-tenant
```

5. **Add replacement to `auth.yaml` and reload.**

### Compromised JWT Secret

1. **Generate new secret immediately**

```bash
openssl rand -base64 32 > jwt_secret_new.txt
```

2. **Restart router with new secret** — All connected agents will be disconnected.

```bash
./bin/hivenet-router --jwt-secret-file jwt_secret_new.txt
```

3. **Restart all agents** with the new secret — they re-authenticate automatically.

```bash
# On each agent server
./bin/hivenet-agent --jwt-secret-file jwt_secret_new.txt --router-grpc <router>:50051
```

4. **Audit access**

```logql
{job="router"} | json | ts > "2026-04-22T12:00:00Z"
```

## Backup and Recovery

### Backup auth.yaml

```bash
# Encrypted backup
gpg -c /etc/hivenet-router/auth.yaml > auth.yaml.gpg

# Or include in regular encrypted backup rotation
tar -czf auth-backup-$(date +%Y%m%d).tar.gz /etc/hivenet-router/auth.yaml
```

### Restore from Backup

```bash
# Restore auth.yaml
cp /backup/auth-backup-20260422.tar.gz /tmp/
tar -xzf /tmp/auth-backup-20260422.tar.gz -C /etc/hivenet-router/

# Reload router
kill -HUP $(pgrep hivenet-router)
```

## See Also

- [Authentication Overview](01-Authentication-Overview.md) - Auth architecture
- [auth.yaml Reference](03-auth.yaml-Reference.md) - Configuration
- [Audit Logging](../Observability/03-Audit-Logging.md) - Usage tracking
