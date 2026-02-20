# QubicDB Documentation

Official documentation for [QubicDB](https://github.com/qubicDB/qubicdb) — a brain-like recursive memory database for LLMs.

## Live

https://qubicdb.github.io/docs/

## Contents

| File | Description |
|------|-------------|
| [API.md](./API.md) | Full API & Operations Reference — REST endpoints, MCP, config, persistence internals, benchmarks |

## Sections in API.md

1. Introduction & architecture overview
2. Data model — Neuron, Synapse, Matrix
3. Lifecycle state machine (Active → Idle → Sleeping → Dormant)
4. Background daemon flow (Decay, Consolidate, Prune, Persist, Reorg)
5. Persistence internals — NRDB binary format + WAL
6. Hybrid search scoring formula (lexical + vector + sentiment)
7. Authentication & authorization (Basic Auth, SHA256 constant-time)
8. Full REST API reference — all endpoints, parameters, error codes
9. MCP endpoint (Streamable HTTP, JSON-RPC 2.0)
10. Configuration reference — YAML, env vars, CLI flags
11. Connection strings (`qubicdb://` scheme)
12. Benchmarks (real results, Apple M3 arm64)
13. Capacity planning & production checklist

## Quick Links

- **Server repo**: [github.com/qubicDB/qubicdb](https://github.com/qubicDB/qubicdb)
- **SDKs**: [github.com/qubicDB/sdks](https://github.com/qubicDB/sdks)
- **Website**: [qubicdb.github.io/qubicdb-web](https://qubicdb.github.io/qubicdb-web/)
- **OpenAPI spec**: [`openapi.yaml`](https://github.com/qubicDB/qubicdb/blob/main/openapi.yaml)

## License

MIT
