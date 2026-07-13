# Native Wrangler authoring

A component is a standard Cloudflare Worker project. The connector reads `wrangler.toml`; there is no `henosis.toml`, marker, or sidecar.

- Component name comes from top-level `name`.
- The module entry comes from `main` (default `src/index.js`).
- Static assets come from `[assets].directory` and are deployed as Workers static assets.
- `dev` and `prod` map to `<name>-dev` and `<name>-prod`.
- `preview_<26-character-id>` maps to `<name>-preview-<first-12-id-characters>`, lowercased. Names longer than 63 characters are truncated with an 8-hex digest suffix.

The authoring boundary receives immutable dependency hashes from graph metadata. It uses those hashes for `depends_on` and for exact upstream-value lookup; names are retained for diagnostics only.

An empty `[vars]` value cannot identify its producer and output from Wrangler alone. That shorthand remains an open question; the explicit interpolation form below is the supported convention.
