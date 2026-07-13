# connector-cloudflare

Cloudflare Workers connector for Henosis. Components are ordinary Wrangler projects; `wrangler.toml` is the only authoring source and marker files are not supported.

See `docs/native-wrangler-authoring.md` and `docs/value-propagation.md`.

The service requires the shared `S2_ACCESS_TOKEN`, `S2_ACCOUNT_ENDPOINT`, `S2_BASIN_ENDPOINT`, and `S2_BASIN` coordinates. `HENOSIS_PLAN_STREAM_PREFIX` defaults to `henosis-plans-v1`; the SDK appends authoritative plans to one `<prefix>-<graphId>` stream per graph. `HENOSIS_STATE_DIR` (default `/var/lib/henosis-connector-cloudflare/state`) is a root; SDK checkpoints and the recoverable plan cache live under `sdk-v1/`, isolated from pre-SDK checkpoint formats.
