# connector-cloudflare

## Layout

- `crates/authoring` derives component specs from native `wrangler.toml` projects.
- `crates/reconciler` owns the protobuf boundary, durable controller, and Wrangler target.
- `services/server` serves the ConnectRPC connector.

## Commands

Use `just`; run `just test` and `just lint` after changes.

## Invariants

- Marker and sidecar files are forbidden.
- Never rewrite a user's Wrangler files to bind values.
- Secret plaintext may only exist inside the target boundary.
- Proto imports belong only in `crates/reconciler/src/proto.rs`.
- Type aliases are forbidden; section comments use `// === NAME ===` when sections are needed.
