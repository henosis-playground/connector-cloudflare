# Value propagation

## Input slots

A string in native `[vars]` is a Henosis slot when it has this exact form:

```toml
[vars]
BACKEND_URL = "${henosis:service-e.url}"
```

The connector resolves only current-generation `upstream_outputs`. If the producer publication or property is absent, the component stays reconciling with diagnostic `cloudflare.input.unbound`, naming both producer and output. Plain values are passed at deployment with `wrangler deploy --var KEY:value`; the native file is never edited.

Outputs ending in `Ref` are secret-reference class values. Their plaintext never travels through core. The target boundary resolves supported `docker-secret://<name>` refs under `HENOSIS_SECRET_ROOT` and writes them with `wrangler secret put`.

## Output value classes

1. **Plan-time:** `url` and `workerName`. The URL is derived before deployment as `https://<deployed-name>.<account-subdomain>.workers.dev`; `url` has output role `ui`.
2. **Apply-time:** `deploymentId`, `versionId`, and optional `claimUrl` are added only after Wrangler succeeds.
3. **Input binding:** slots consume the current generation's upstream output values as described above.

The account subdomain is fetched from Cloudflare's account API (or supplied explicitly for deterministic local testing).

## Explicit plans

Every reconciliation appends `henosis.dev/connector-plan/v1` to the SDK's authoritative per-graph S2 stream before Wrangler or the Cloudflare API may mutate anything. The private plan lists Worker/Tunnel creates or updates, removed-resource deletions, and the exact `[vars]` keys being bound. Its redacted JSON and Markdown projections ride in the same content-addressed record.

`deploymentId`, `versionId`, optional `claimUrl`, and a URL whose account subdomain is unavailable at plan time are declared as `unknownSlots`; they are never presented as known values. Missing producer values produce a durable `blocked` plan with `blockedOnInputs` entries of the form `<producer spec hash>.<output>`, and no apply is scheduled.
