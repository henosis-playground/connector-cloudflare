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

The account subdomain is fetched once from Cloudflare's account API per connector process (or supplied explicitly for deterministic local testing).
