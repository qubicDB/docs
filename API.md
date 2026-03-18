# QubicDB API & Operations Reference

> **Companion documents:**
> - OpenAPI contract: [`./openapi.yaml`](./openapi.yaml)
> - Server source: `pkg/api/server.go`
> - Error model: `pkg/api/apierr/errors.go`
> - Config model: `pkg/core/brain.go`
> - Persistence: `pkg/persistence/store.go`, `pkg/persistence/codec.go`
> - Search engine: `pkg/engine/search.go`, `pkg/engine/matrix_ops.go`

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Architecture Overview](#2-architecture-overview)
3. [Data Model — Neuron, Synapse, Matrix](#3-data-model--neuron-synapse-matrix)
4. [Lifecycle State Machine](#4-lifecycle-state-machine)
5. [Background Daemon Flow](#5-background-daemon-flow)
6. [Persistence Internals — NRDB Format](#6-persistence-internals--nrdb-format)
7. [Hybrid Search — Scoring Formula](#7-hybrid-search--scoring-formula)
8. [Contract Boundaries](#8-contract-boundaries)
9. [Authentication & Authorization](#9-authentication--authorization)
10. [Request Safety Controls](#10-request-safety-controls)
11. [Memory API Behavior](#11-memory-api-behavior)
12. [Brain Lifecycle API](#12-brain-lifecycle-api)
13. [Command API](#13-command-api)
14. [Registry API](#14-registry-api)
15. [Admin API](#15-admin-api)
16. [Runtime Config API](#16-runtime-config-api)
17. [Error Model & Code Catalog](#17-error-model--code-catalog)
18. [Configuration Reference](#18-configuration-reference)
19. [Connection Strings](#19-connection-strings)
20. [MCP Endpoint](#20-mcp-endpoint)
21. [Benchmarks](#21-benchmarks)
22. [Capacity Planning & Tuning](#22-capacity-planning--tuning)
23. [Production Checklist](#23-production-checklist)

---

## 1) Introduction

### What is QubicDB?

QubicDB is a **brain-like recursive memory database** for LLM applications, written in Go. It is not a vector store wrapper, not a key-value cache, and not a document database. It is a **per-index memory runtime** where each index owns an isolated in-memory `Matrix` (N-dimensional neuron space), a dedicated serialized `BrainWorker` goroutine, a lifecycle state machine (Active → Idle → Sleeping → Dormant), background daemons (Decay, Consolidate, Prune, Persist, Reorg), and a hybrid lexical + vector search engine with brain-mechanics ranking.

### What problem does it solve?

| Problem | Root cause | QubicDB answer |
|---|---|---|
| Session cold start | No memory continuity across turns | Per-index persistent matrix, auto-loaded on access |
| Shallow retrieval | Flat similarity, no behavioral weighting | Energy/recency/access/depth multipliers on every score |
| Multi-tenant bleed | Shared memory paths | Per-index worker isolation, no shared state |
| Ops gap | No lifecycle or admin surface | Full admin API, lifecycle control, runtime config patch |
| Semantic gap | Pure lexical or pure vector | Hybrid α-blended scoring with graceful fallback |

### What's new in the current production surface

- **Hybrid search** — lexical + GGUF embedding vector scoring, α-weighted, SIMD cosine similarity
- **Runtime config patch** — `POST /v1/config` with explicit safe-field whitelist, partial apply
- **Registry guard** — `registry.enabled` for controlled index admission
- **Config-driven admin auth** — Basic Auth with SHA-256 constant-time comparison
- **Standardized error envelope** — machine-readable `code` field, stable across versions
- **MCP endpoint** — optional streamable HTTP MCP with per-tool allowlist and dedicated rate limiter
- **Connection strings** — `qubicdb://` and `qubicdb+tls://` for SDK/CLI interoperability
- **WAL + fsync policy** — `always | interval | off` with startup repair
- **Checksum validation** — CRC32 on every `.nrdb` file, optional background scan interval

---

## 2) Architecture Overview

```
HTTP Client / SDK / MCP Client
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  pkg/api/server.go  (HTTP REST, mux, middleware)    │
│  Rate Limiter │ CORS Headers │ Admin Basic Auth     │
└───────────────────────┬─────────────────────────────┘
                        │ index-scoped request
                        ▼
┌─────────────────────────────────────────────────────┐
│  pkg/concurrency/worker_pool.go  (WorkerPool)       │
│  BrainWorker["index-A"]  (goroutine + queue)        │
│  BrainWorker["index-B"]  (goroutine + queue)        │
└───────────────────────┬─────────────────────────────┘
                        │ serialized operations
                        ▼
┌─────────────────────────────────────────────────────┐
│  pkg/engine/matrix_ops.go  (MatrixEngine)           │
│  AddNeuron │ GetNeuron │ Searcher (hybrid score)    │
│  spreadActivation │ tokenCache                      │
└──────────────┬──────────────────┬───────────────────┘
               ▼                  ▼
┌──────────────────────┐  ┌──────────────────────────┐
│  pkg/core/types.go   │  │  pkg/vector/vectorizer   │
│  Matrix/Neuron/      │  │  GGUF embedding (llama)  │
│  Synapse/BrainState  │  │  SIMD cosine similarity  │
└──────────┬───────────┘  └──────────────────────────┘
           ▼
┌─────────────────────────────────────────────────────┐
│  pkg/persistence/store.go  (Store)                  │
│  WAL (msgpack) + .nrdb files                        │
│  CRC32 checksum + optional gzip                     │
│  fsync policy: always | interval | off              │
└─────────────────────────────────────────────────────┘
```

**Key design decisions:**
- **One worker per index** — all operations serialized through a single goroutine queue. No per-index mutex contention from the API layer.
- **Organic matrix** — dimension grows as neurons are added (up to `matrix.maxDimension`). Spatial positions are N-dimensional `float64` vectors.
- **Vectorizer is global** — loaded once in `main.go`, passed through `WorkerPool → BrainWorker → MatrixEngine/Searcher`. If unavailable, search degrades to lexical.
- **Registry is optional** — when `registry.enabled=false` (default), any UUID can create an index.

---

## 3) Data Model — Neuron, Synapse, Matrix

### Neuron

| Field | Type | Description |
|---|---|---|
| `id` | `string` (UUID) | Unique identifier, auto-generated |
| `content` | `string` | The stored text/memory content |
| `content_hash` | `string` | SHA-1 UUID of content — used for deduplication |
| `energy` | `float64` | Current activation level `[0.0, 1.0]`. Born at `1.0`. |
| `base_energy` | `float64` | Resting energy floor. Default `0.1`. |
| `depth` | `int` | Memory hierarchy depth. `0` = surface/hot. Higher = consolidated. |
| `created_at` | `time` | Creation timestamp |
| `last_fired_at` | `time` | Last activation timestamp — recency scoring |
| `access_count` | `uint64` | Total access/fire count — importance scoring |
| `tags` | `[]string` | Emergent classification tags |
| `embedding` | `[]float32` | 384-dim MiniLM vector. Set once on write. `nil` if vector disabled. |
| `metadata` | `map[string]any` | Arbitrary key-value metadata |

**Neuron lifecycle:**
- Born fully activated (`energy=1.0`)
- Decays: `energy -= rate * elapsedHours / 3600`
- Fired on search hit: `energy = min(1.0, energy + 0.3)`
- `IsAlive()` → `energy > 0.01` | `IsDormant()` → `0 < energy ≤ 0.01`
- Neurons are **never deleted** by decay — they become dormant
- **Deduplication:** duplicate `content_hash` fires existing neuron instead of inserting

### Synapse

| Field | Type | Description |
|---|---|---|
| `id` | `string` | `"fromID:toID"` composite key |
| `weight` | `float64` | Connection strength `[0.0, 1.0]` |
| `co_fire_count` | `uint64` | Times both neurons fired together |
| `bidirectional` | `bool` | Default `true` — traversed both ways in spread activation |

**Hebbian learning:** Synapses strengthen on co-activation, weaken over time. `weight < 0.05` = weak. `weight < 0.01` + inactive 30d = archive candidate.

### Matrix

| Field | Type | Description |
|---|---|---|
| `index_id` | `string` | Owner index identifier |
| `current_dim` | `int` | Current dimensionality (grows, bounded by `maxDimension`) |
| `neurons` | `map[NeuronID]*Neuron` | All neurons |
| `synapses` | `map[SynapseID]*Synapse` | All synapses |
| `adjacency` | `map[NeuronID][]NeuronID` | Fast traversal for spread activation |
| `decay_rate` | `float64` | Self-tuning (default `0.1`) |
| `version` | `uint64` | Monotonic — used by persistence layer |

---

## 4) Lifecycle State Machine

```
                    any operation
  ┌──────────────────────────────────────────────────────────────┐
  │                                                              │
  ▼                                                              │
Active ──(idleThreshold=30s)──► Idle ──(sleepThreshold=5m)──► Sleeping ──(dormantThreshold=30m)──► Dormant
  ▲                                                              │                                    │
  └──────────────────── wake() ◄────────────────────────────────┴────────────────────────────────────┘
```

| State | Value | Meaning |
|---|---|---|
| `active` | 0 | Brain is in active use |
| `idle` | 1 | No activity for `idleThreshold` |
| `sleeping` | 2 | Consolidation in progress |
| `dormant` | 3 | Eligible for worker eviction |

**Wake behavior:** Any incoming operation auto-wakes a sleeping/dormant index. Workers idle beyond `worker.maxIdleTime` (default 30m) are evicted; matrix is reloaded from disk on next access.

**Lifecycle thresholds are runtime-patchable** via `POST /v1/config`.

---

## 5) Background Daemon Flow

Five background goroutines run per server instance, iterating over all active workers on each tick.

| Daemon | Interval | What it does |
|---|---|---|
| **Decay** | `daemons.decayInterval` (1m) | Reduces neuron energy and synapse weight over time |
| **Consolidate** | `daemons.consolidateInterval` (5m) | Moves frequently-accessed old neurons to deeper depth |
| **Prune** | `daemons.pruneInterval` (10m) | Archives dead synapses (`weight<0.01`, inactive 30d) |
| **Persist** | `daemons.persistInterval` (1m) | Flushes pending writes to `.nrdb` + WAL + fsync |
| **Reorg** | `daemons.reorgInterval` (15m) | Optimizes spatial locality, expands/contracts matrix dim |

**Admin control:** `POST /admin/daemons/pause` | `POST /admin/daemons/resume` | `GET /admin/daemons`

**Boundary guards (from `Validate()`):**
- `decayInterval < 5s` → warning (high CPU)
- `persistInterval < 5s` → warning (high disk I/O)

---

## 6) Persistence Internals — NRDB Format

Each index is persisted as a single `.nrdb` binary file in `storage.dataPath`.

### File format

```
Header (24 bytes, little-endian):
  [4]  Magic    = "NRDB"
  [2]  Version  = 1
  [2]  Flags    = bit0: gzip compressed | bit1: encrypted (reserved)
  [4]  IndexIDLen
  [8]  DataLen
  [4]  Checksum = CRC32 of payload

[IndexIDLen bytes]  IndexID string

[DataLen bytes]     Payload = msgpack(Matrix) [optionally gzip-compressed]
```

### WAL (Write-Ahead Log)

When `storage.walEnabled=true`, every write is appended as a msgpack `walRecord{Op, IndexID, Data}` before the `.nrdb` flush. On startup with `storage.startupRepair=true`, the WAL is replayed to recover unflushed writes.

### fsync policy

| Policy | Behavior | Use case |
|---|---|---|
| `always` | fsync after every write | Maximum durability, high I/O cost |
| `interval` | fsync every `fsyncInterval` (default 1s) | Balanced (default) |
| `off` | No fsync | Maximum throughput, crash risk |

### Snapshot

A lightweight `Snapshot{IndexID, Version, NeuronCount, SynapseCount, CurrentDim, TotalEnergy, ModifiedAt}` is maintained in memory for fast stats queries without deserializing the full matrix.

---

## 7) Hybrid Search — Scoring Formula

### Stage 1: Query preparation

```
tokenize(query)                          → queryTokens []string
strings.ToLower(query)                   → queryLower
vectorizer.EmbedText(query)+Normalize()  → queryVec []float32
  (done outside matrix lock)
```

### Stage 2: String score (lexical)

```
exactPhraseScore:
  strings.Contains(content, queryLower) → +10.0

wordOverlapScore:
  matchedWords = count(queryTokens found in contentTokens, exact or prefix-3 fuzzy)
  score += (matchedWords / len(queryTokens)) * 5.0
  score += 0.3 per fuzzy prefix match

fuzzyLevenshteinScore (only if query ≤ 20 chars AND stringScore < 8.0):
  for first 8 content tokens:
    similarity = 1 - levenshtein(queryLower, token) / max(len(q), len(t))
    if similarity > 0.7 → score += similarity * 2.0
```

### Stage 3: Hybrid base score

```
vectorScore = max(CosineSimilarity(queryVec, neuron.Embedding), 0.0)
  (SIMD-accelerated on arm64/amd64)

if both queryVec and neuron.Embedding available:
  normStringScore = stringScore / (stringScore + 1.0)   ← sigmoid normalization
  baseScore = α × vectorScore + (1-α) × normStringScore
  where α = vector.alpha (default 0.6)
else:
  baseScore = stringScore   ← pure lexical fallback
```

### Stage 4: Brain-mechanics multipliers

```
× (0.5 + energy × 0.5)                      ← energy boost
× (0.8 + 0.2 / (1 + ageHours / 24))         ← recency boost
× (1.0 + 0.1 × log10(accessCount + 1))      ← access importance
× (1.0 / (1 + depth × 0.2))                 ← depth penalty
× SentimentBoost(queryLabel, neuronLabel)    ← sentiment boost [0.8, 1.2]
× (1.0 + matchCount × 0.3)                  ← metadata boost (+30% per matching key)
```

**Sentiment boost:** Applied when query and neuron share a non-neutral sentiment label (happiness, sadness, fear, anger, disgust, surprise). Range: `[0.8, 1.2]`.

**Metadata boost:** Applied when `metadata` filter is provided and `strict=false`. Each matching key-value pair adds 30% to the score. Neurons with no metadata are not penalized.

Neurons with `finalScore ≤ 0` are excluded.

### Stage 5: Spread activation (depth > 0)

```
for d in 0..depth:
  for each result neuron r:
    for each neighbor n in adjacency[r.ID]:
      spreadScore = r.Score × synapseWeight × (1 / (d + 2))
      if spreadScore > 0.1 → include n in results
```

**Stage 6: Strict metadata post-filter** (when `strict=true`)

After spread activation, neurons that do not match ALL specified metadata key-value pairs are removed from results. This ensures spread-activated neighbors that don't match are excluded.

Results re-sorted after spread. Neurons are fired (`energy += 0.3`) after selection.

### Alpha tuning guide

| α | Effect |
|---|---|
| `0.0` | Pure lexical |
| `0.3` | Lexical-dominant with semantic assist |
| `0.6` | Balanced hybrid (default) |
| `0.8` | Semantic-dominant |
| `1.0` | Pure vector |

`vector.alpha` is patchable at runtime via `POST /v1/config`.

---

## 8) Contract Boundaries

### Protocols exposed
- **HTTP/REST**: `server.httpAddr` (default `:6060`)
- **MCP** (optional): `mcp.path` (default `/mcp`, disabled by default)

### Index scoping (critical)
Most brain/memory endpoints are index-scoped. QubicDB resolves index ID in this order:

1. `X-Index-ID` header (preferred)
2. `indexId` query param
3. `index_id` query param

Missing → HTTP `400`, code `INDEX_ID_REQUIRED`

### Registry guard
If `registry.enabled=true`, index-scoped operations are allowed **only** for registered UUIDs.
Unregistered → HTTP `400`, code `UUID_NOT_REGISTERED`

---

## 9) Authentication & Authorization

### Admin auth
When `admin.enabled=true`, admin routes are protected by HTTP Basic Auth:
- Username: `admin.user` (default `admin`)
- Password: `admin.password` (default `qubicdb` — **change in production**)
- Comparison: SHA-256 constant-time (`subtle.ConstantTimeCompare`)

`POST /admin/login` validates credentials and returns success. It does **not** issue a session or token — client must send Basic Auth on every protected request.

Missing/invalid credentials → HTTP `401`, code `UNAUTHORIZED`

`admin.enabled=false` → admin endpoints not mounted, return `404`

### MCP auth
Accepts `X-API-Key` header or `Authorization: Bearer <key>` when `mcp.apiKey` is set.

---

## 10) Request Safety Controls

| Control | Config key | Default | On violation |
|---|---|---|---|
| Rate limiter | (built-in) | 300 req/min/client | HTTP 429, `RATE_LIMITED`, `Retry-After` |
| Request body limit | `security.maxRequestBody` | 1 MB | HTTP 413, `PAYLOAD_TOO_LARGE` |
| Neuron content limit | `security.maxNeuronContentBytes` | 64 KB | HTTP 413, `PAYLOAD_TOO_LARGE` |
| CORS | `security.allowedOrigins` | `http://localhost:6060` | — |
| Read timeout | `security.readTimeout` | 30s | connection closed |
| Write timeout | `security.writeTimeout` | 30s | connection closed |

Client key resolution order: `X-Forwarded-For` first IP → `X-Real-IP` → remote address host

**CORS constraint:** `allowedOrigins` must not be `*` when `admin.enabled=true` — enforced by `Validate()`.

---

## 11) Memory API Behavior

### `POST /v1/write`

```json
{
  "content": "string (required)",
  "parent_id": "uuid (optional)",
  "metadata": { "thread_id": "conv-1", "role": "user" },
  "tags": []
}
```

- `metadata` is an optional `map[string]string` — arbitrary key-value pairs (e.g. `thread_id`, `role`, `source`, `dataset_name`). Stored on the neuron and returned in all responses.
- `parent_id` positions the new neuron spatially near the parent.
- Duplicate `content_hash` → fires existing neuron, returns it (no duplicate insert)
- Vector layer enabled → auto-embeds via `vectorizer.EmbedText` + `Normalize`, stored in `neuron.embedding`
- Sentiment layer → auto-labels `sentiment_label` + `sentiment_score` on write
- Response: full neuron document including `metadata`

### `GET /v1/read/{id}`

Reads one neuron by ID. Fires the neuron on access.
Not found → HTTP `404`, code `NEURON_NOT_FOUND`

### `GET /v1/recall`

Lists neurons. Returns first 100. Response aliases: `memories` / `neurons` (same data).

### `GET /v1/search` or `POST /v1/search`

| Param | Type | Default | Server max | Notes |
|---|---|---|---|---|
| `q` / `query` | string | — | — | Required |
| `depth` | int | `2` | `8` | Spread activation depth |
| `limit` | int | `20` | `200` | Max results |
| `metadata` | `map[string]string` | `null` | — | Optional filter/boost |
| `strict` | bool | `false` | — | Strict metadata filter |

**Metadata behavior:**
- `strict=false` (default): neurons matching metadata keys are boosted (+30% score per matching key). All neurons remain eligible.
- `strict=true`: only neurons matching **all** specified key-value pairs are returned. Applied after spread activation.

**GET query param syntax:** `?q=query&metadata_thread_id=conv-1&strict=true`

Empty query → HTTP `400`, code `QUERY_REQUIRED`

### `POST /v1/context`

Builds token-budgeted context string from a search cue.

| Param | Default | Server max |
|---|---|---|
| `cue` | — | — |
| `maxTokens` | `2000` | `16000` |
| `depth` | `2` | `8` |

Token estimation: `estimatedTokens += len(content) / 4`

Response aliases: `context`/`text`, `neuronsUsed`/`neuronCount`, `estimatedTokens`/`tokenCount`

### Intentionally disabled mutation endpoints

Memory mutation must remain organic/internal. These routes always reject:

| Route | HTTP | Code |
|---|---|---|
| `PUT /v1/touch` | 400 | `MUTATION_DISABLED` |
| `DELETE /v1/forget/{id}` | 400 | `MUTATION_DISABLED` |
| `POST /v1/fire/{id}` | 400 | `MUTATION_DISABLED` |

---

## 12) Brain Lifecycle API

All routes are index-scoped.

| Route | Description |
|---|---|
| `GET /v1/brain/state` | Returns current lifecycle state and timestamps |
| `POST /v1/brain/wake` | Force-wake a sleeping/dormant brain |
| `POST /v1/brain/sleep` | Force-sleep an active brain |
| `GET /v1/brain/stats` | Returns matrix stats (neuron/synapse count, energy, dim) |

State values: `active` (0), `idle` (1), `sleeping` (2), `dormant` (3)

---

## 13) Command API

### `POST /v1/command`

MongoDB-like command envelope:

```json
{
  "type": "find",
  "collection": "neurons",
  "filter": { "energy": { "$gt": 0.5 } },
  "options": { "limit": 10 }
}
```

**Supported read commands:** `find`, `findOne`, `count`, `search`, `stats`, `insert`

**Blocked mutation commands:** `update`, `updateOne`, `delete`, `deleteOne`, `activate`
→ HTTP `400`, code `MUTATION_DISABLED`

**Filter operators:** `$gt`, `$lt`, `$gte`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$regex`, `$contains`

---

## 14) Registry API

| Route | Description |
|---|---|
| `GET /v1/registry` | List all registered UUIDs |
| `POST /v1/registry` | Register a new UUID |
| `GET /v1/registry/{uuid}` | Get a specific registry entry |
| `PUT /v1/registry/{uuid}` | Update a registry entry |
| `DELETE /v1/registry/{uuid}` | Remove a registry entry |
| `POST /v1/registry/find-or-create` | Idempotent find-or-create |

- Duplicate UUID → HTTP `409`, code `UUID_CONFLICT`
- Missing UUID → HTTP `404`, code `UUID_NOT_FOUND`

---

## 15) Admin API

Mounted only when `admin.enabled=true`. All routes require Basic Auth.

| Route | Description |
|---|---|
| `POST /admin/login` | Validate credentials |
| `GET /admin/indexes` | List all indexes with stats |
| `GET /admin/indexes/{indexId}` | Get index detail |
| `DELETE /admin/indexes/{indexId}` | Delete index (data + lifecycle + registry entry) |
| `POST /admin/indexes/{indexId}/reset` | Reset index data (keeps registry entry) |
| `POST /admin/indexes/{indexId}/wake` | Force-wake index |
| `POST /admin/indexes/{indexId}/sleep` | Force-sleep index |
| `GET /admin/indexes/{indexId}/export` | Export index data |
| `GET /admin/daemons` | Inspect daemon status |
| `POST /admin/daemons/pause` | Pause all background daemons |
| `POST /admin/daemons/resume` | Resume all background daemons |
| `POST /admin/gc` | Trigger garbage collection |
| `POST /admin/persist` | Force flush all pending writes to disk |

**Delete vs Reset:**
- `DELETE` → truncates data + removes lifecycle state + deletes registry entry
- `POST .../reset` → truncates data only, keeps registry entry

---

## 16) Runtime Config API

| Route | Description |
|---|---|
| `GET /v1/config` | Get full current config (alias: `GET /admin/config`) |
| `POST /v1/config` | Patch runtime config (alias: `POST /admin/config`) |

### Runtime patch whitelist

Only these fields are patchable at runtime (others are rejected):

```
lifecycle.idleThreshold
lifecycle.sleepThreshold
lifecycle.dormantThreshold
daemons.decayInterval
daemons.consolidateInterval
daemons.pruneInterval
daemons.persistInterval
daemons.reorgInterval
worker.maxIdleTime
registry.enabled
matrix.maxNeurons
security.allowedOrigins
security.maxRequestBody
vector.alpha
```

**Patch behavior:**
- Partially valid patches succeed (`200`) with both `changed` and `rejected` lists
- Nothing valid applied → HTTP `400`, code `BAD_REQUEST`

---

## 17) Error Model & Code Catalog

All API errors use one envelope:

```json
{
  "ok": false,
  "error": "human-readable message",
  "code": "MACHINE_READABLE_CODE",
  "status": 400
}
```

**Always branch on `code`, not on `error` message text.**

### General codes

| Code | HTTP | Meaning |
|---|---|---|
| `BAD_REQUEST` | 400 | Malformed request |
| `INVALID_JSON` | 400 | Unparseable JSON body |
| `INVALID_CONTENT` | 400 | Content validation failed |
| `PAYLOAD_TOO_LARGE` | 413 | Body or neuron content exceeds limit |
| `METHOD_NOT_ALLOWED` | 405 | Wrong HTTP method |
| `NOT_FOUND` | 404 | Route not found |
| `INTERNAL_ERROR` | 500 | Unexpected server error |
| `UNAUTHORIZED` | 401 | Missing or invalid Basic Auth |
| `RATE_LIMITED` | 429 | Rate limit exceeded |
| `CONFLICT` | 409 | Generic conflict |
| `MUTATION_DISABLED` | 400 | Direct mutation is intentionally blocked |

### Domain codes

| Code | HTTP | Meaning |
|---|---|---|
| `INDEX_ID_REQUIRED` | 400 | No index selector provided |
| `NEURON_ID_REQUIRED` | 400 | No neuron ID in path |
| `NEURON_NOT_FOUND` | 404 | Neuron ID does not exist |
| `QUERY_REQUIRED` | 400 | Empty search query |
| `UUID_REQUIRED` | 400 | No UUID provided |
| `UUID_NOT_REGISTERED` | 400 | Registry guard rejected unregistered UUID |
| `UUID_NOT_FOUND` | 404 | Registry entry not found |
| `UUID_CONFLICT` | 409 | UUID already registered |

---

## 18) Configuration Reference

### Merge hierarchy (highest precedence first)

```
1. CLI flags          (--http-addr, --admin-password, etc.)
2. YAML config file   (--config /path/to/config.yaml)
3. Environment vars   (QUBICDB_* prefix)
4. Built-in defaults
```

### Full defaults table

| Config key | Default | Notes |
|---|---|---|
| `server.httpAddr` | `:6060` | |
| `storage.dataPath` | `./data` | |
| `storage.compress` | `true` | gzip on `.nrdb` payload |
| `storage.walEnabled` | `true` | Write-Ahead Log |
| `storage.fsyncPolicy` | `interval` | `always\|interval\|off` |
| `storage.fsyncInterval` | `1s` | Used when policy=interval |
| `storage.checksumValidationInterval` | `0` | `0` = disabled |
| `storage.startupRepair` | `true` | WAL replay on startup |
| `matrix.minDimension` | `3` | Initial matrix dimensionality |
| `matrix.maxDimension` | `1000` | Hard cap on matrix growth |
| `matrix.maxNeurons` | `1,000,000` | Hard cap per index |
| `lifecycle.idleThreshold` | `30s` | Active → Idle |
| `lifecycle.sleepThreshold` | `5m` | Idle → Sleeping |
| `lifecycle.dormantThreshold` | `30m` | Sleeping → Dormant |
| `daemons.decayInterval` | `1m` | |
| `daemons.consolidateInterval` | `5m` | |
| `daemons.pruneInterval` | `10m` | |
| `daemons.persistInterval` | `1m` | |
| `daemons.reorgInterval` | `15m` | |
| `worker.maxIdleTime` | `30m` | Worker eviction threshold |
| `registry.enabled` | `false` | UUID guard off by default |
| `vector.enabled` | `true` | |
| `vector.modelPath` | `./dist/MiniLM-L6-v2.Q8_0.gguf` | GGUF BERT model |
| `vector.gpuLayers` | `0` | `0` = CPU only |
| `vector.alpha` | `0.6` | Hybrid search weight |
| `admin.enabled` | `true` | |
| `admin.user` | `admin` | |
| `admin.password` | `qubicdb` | **Change in production** |
| `mcp.enabled` | `false` | |
| `mcp.path` | `/mcp` | |
| `mcp.apiKey` | `""` | Empty = unauthenticated |
| `mcp.stateless` | `true` | |
| `mcp.rateLimitRPS` | `30` | |
| `mcp.rateLimitBurst` | `60` | |
| `mcp.enablePrompts` | `true` | |
| `security.allowedOrigins` | `http://localhost:6060` | |
| `security.maxRequestBody` | `1,048,576` | 1 MB |
| `security.maxNeuronContentBytes` | `65,536` | 64 KB |
| `security.readTimeout` | `30s` | |
| `security.writeTimeout` | `30s` | |

### Environment variable mapping

| Env var | Config key |
|---|---|
| `QUBICDB_HTTP_ADDR` | `server.httpAddr` |
| `QUBICDB_DATA_PATH` | `storage.dataPath` |
| `QUBICDB_COMPRESS` | `storage.compress` |
| `QUBICDB_WAL_ENABLED` | `storage.walEnabled` |
| `QUBICDB_FSYNC_POLICY` | `storage.fsyncPolicy` |
| `QUBICDB_FSYNC_INTERVAL` | `storage.fsyncInterval` |
| `QUBICDB_CHECKSUM_VALIDATION_INTERVAL` | `storage.checksumValidationInterval` |
| `QUBICDB_STARTUP_REPAIR` | `storage.startupRepair` |
| `QUBICDB_MIN_DIMENSION` | `matrix.minDimension` |
| `QUBICDB_MAX_DIMENSION` | `matrix.maxDimension` |
| `QUBICDB_MAX_NEURONS` | `matrix.maxNeurons` |
| `QUBICDB_IDLE_THRESHOLD` | `lifecycle.idleThreshold` |
| `QUBICDB_SLEEP_THRESHOLD` | `lifecycle.sleepThreshold` |
| `QUBICDB_DORMANT_THRESHOLD` | `lifecycle.dormantThreshold` |
| `QUBICDB_DECAY_INTERVAL` | `daemons.decayInterval` |
| `QUBICDB_CONSOLIDATE_INTERVAL` | `daemons.consolidateInterval` |
| `QUBICDB_PRUNE_INTERVAL` | `daemons.pruneInterval` |
| `QUBICDB_PERSIST_INTERVAL` | `daemons.persistInterval` |
| `QUBICDB_REORG_INTERVAL` | `daemons.reorgInterval` |
| `QUBICDB_MAX_IDLE_TIME` | `worker.maxIdleTime` |
| `QUBICDB_REGISTRY_ENABLED` | `registry.enabled` |
| `QUBICDB_VECTOR_ENABLED` | `vector.enabled` |
| `QUBICDB_VECTOR_MODEL_PATH` | `vector.modelPath` |
| `QUBICDB_VECTOR_GPU_LAYERS` | `vector.gpuLayers` |
| `QUBICDB_VECTOR_ALPHA` | `vector.alpha` |
| `QUBICDB_ADMIN_ENABLED` | `admin.enabled` |
| `QUBICDB_ADMIN_USER` | `admin.user` |
| `QUBICDB_ADMIN_PASSWORD` | `admin.password` |
| `QUBICDB_MCP_ENABLED` | `mcp.enabled` |
| `QUBICDB_MCP_PATH` | `mcp.path` |
| `QUBICDB_MCP_API_KEY` | `mcp.apiKey` |
| `QUBICDB_MCP_STATELESS` | `mcp.stateless` |
| `QUBICDB_MCP_RATE_LIMIT_RPS` | `mcp.rateLimitRPS` |
| `QUBICDB_MCP_RATE_LIMIT_BURST` | `mcp.rateLimitBurst` |
| `QUBICDB_MCP_ENABLE_PROMPTS` | `mcp.enablePrompts` |
| `QUBICDB_MCP_ALLOWED_TOOLS` | `mcp.allowedTools` (comma-separated) |
| `QUBICDB_ALLOWED_ORIGINS` | `security.allowedOrigins` |
| `QUBICDB_MAX_REQUEST_BODY` | `security.maxRequestBody` |
| `QUBICDB_MAX_NEURON_CONTENT_BYTES` | `security.maxNeuronContentBytes` |
| `QUBICDB_TLS_CERT` | `security.tlsCert` |
| `QUBICDB_TLS_KEY` | `security.tlsKey` |
| `QUBICDB_READ_TIMEOUT` | `security.readTimeout` |
| `QUBICDB_WRITE_TIMEOUT` | `security.writeTimeout` |

### CLI flags

| Flag | Config key |
|---|---|
| `--config` | config file path |
| `--http-addr` | `server.httpAddr` |
| `--data-path` | `storage.dataPath` |
| `--compress` | `storage.compress` |
| `--min-dimension` | `matrix.minDimension` |
| `--max-dimension` | `matrix.maxDimension` |
| `--max-neurons` | `matrix.maxNeurons` |
| `--registry` | `registry.enabled` |
| `--vector` | `vector.enabled` |
| `--vector-model` | `vector.modelPath` |
| `--vector-gpu-layers` | `vector.gpuLayers` |
| `--vector-alpha` | `vector.alpha` |
| `--admin` | `admin.enabled` |
| `--admin-user` | `admin.user` |
| `--admin-password` | `admin.password` |
| `--allowed-origins` | `security.allowedOrigins` |
| `--max-neuron-content-bytes` | `security.maxNeuronContentBytes` |
| `--tls-cert` | `security.tlsCert` |
| `--tls-key` | `security.tlsKey` |

### Validation rules enforced by `Validate()`

- `idleThreshold < sleepThreshold < dormantThreshold` (ordering enforced)
- `minDimension ≤ maxDimension`
- `fsyncPolicy` must be `always | interval | off`
- `fsyncInterval > 0` when policy is `interval`
- `admin.password` must not be default in production (`QUBICDB_ENV=production`)
- `allowedOrigins` must not be `*` when `admin.enabled=true`
- `tlsCert` and `tlsKey` must both be set or both empty
- `vector.alpha` must be in `[0.0, 1.0]`
- Boundary warnings: `maxNeurons > 10M`, `maxDimension > 10K`, `decayInterval < 5s`, `persistInterval < 5s`

---

## 19) Connection Strings

### Format

```
qubicdb://[user:password@]host1[:port1][,host2[:port2]...][/indexID]
qubicdb+tls://[user:password@]host1[:port1][,host2[:port2]...][/indexID]
```

### Parsed fields

| Field | Description |
|---|---|
| `scheme` | `qubicdb` or `qubicdb+tls` |
| `user` | Optional username |
| `password` | Optional password |
| `hosts` | `[]string` — one or more `host:port` |
| `indexID` | Optional default index ID (path component) |
| `tls` | `true` when scheme is `qubicdb+tls` |

### Behavior

- Missing port defaults to `6060`
- `qubicdb+tls://` → clients use HTTPS base URL
- Multiple hosts for future multi-node routing (currently first host is used)

### Examples

```
qubicdb://localhost/my-index
qubicdb://admin:secret@prod.example.com:6060/user-123
qubicdb+tls://admin:secret@prod.example.com/user-123
qubicdb://host1:6060,host2:6060/shared-index
```

---

## 20) MCP Endpoint

When `mcp.enabled=true`, the MCP server mounts at `mcp.path` (default `/mcp`) using streamable HTTP transport.

### Built-in tools

#### Single-Index Tools

| Tool | Description |
|---|---|
| `qubicdb_write` | Write a memory to an index |
| `qubicdb_read` | Read a neuron by ID |
| `qubicdb_search` | Hybrid search with optional metadata boost/filter |
| `qubicdb_recall` | List recent memories |
| `qubicdb_context` | Build token-budgeted context from a cue |
| `qubicdb_registry_find_or_create` | Idempotent registry entry |

#### Cross-Index / Global Tools

| Tool | Description |
|---|---|
| `qubicdb_list_indexes` | List all registered indexes with stats (neuron count, last op, metadata) |
| `qubicdb_global_search` | Search across ALL active indexes with semantic/vector similarity |
| `qubicdb_multi_search` | Search across a specific list of indexes |
| `qubicdb_recent_indexes` | Get most recently active indexes sorted by last operation time |

### Cross-Index Workflow Example

```
1. qubicdb_list_indexes(active_only: true)        → discover available brains
2. qubicdb_recent_indexes(limit: 10, min_neurons: 5) → find most active ones
3. qubicdb_global_search(query: "authentication") → search across all indexes
4. qubicdb_multi_search(index_ids: ["brain-repo1","brain-repo2"], query: "API") → targeted search
```

### Tool parameters

**`qubicdb_write`**

| Param | Required | Description |
|---|---|---|
| `index_id` | ✓ | QubicDB index id |
| `content` | ✓ | Memory content |
| `metadata` | — | JSON string of `{"key":"value"}` pairs (e.g. `{"thread_id":"conv-1","role":"user"}`) |

**`qubicdb_search`**

| Param | Required | Description |
|---|---|---|
| `index_id` | ✓ | QubicDB index id |
| `query` | ✓ | Search query |
| `depth` | — | Search depth (default 2) |
| `limit` | — | Result limit (default 20) |
| `metadata` | — | JSON string of `{"key":"value"}` pairs for filter/boost |
| `strict` | — | `true` = hard filter, `false` = soft boost (default) |

**`qubicdb_list_indexes`**

| Param | Required | Description |
|---|---|---|
| `active_only` | — | Only return currently loaded/active indexes (default `false`) |
| `limit` | — | Max indexes to return (default 100) |

**`qubicdb_global_search`**

| Param | Required | Description |
|---|---|---|
| `query` | ✓ | Search query |
| `depth` | — | Search depth (default 2) |
| `limit` | — | Result limit per index (default 10) |
| `metadata` | — | JSON string of `{"key":"value"}` pairs for filter/boost |

**`qubicdb_multi_search`**

| Param | Required | Description |
|---|---|---|
| `index_ids` | ✓ | JSON array of index IDs to search (e.g. `["brain-repo1","brain-repo2"]`) |
| `query` | ✓ | Search query |
| `depth` | — | Search depth (default 2) |
| `limit` | — | Result limit per index (default 10) |
| `metadata` | — | JSON string of `{"key":"value"}` pairs for filter/boost |

**`qubicdb_recent_indexes`**

| Param | Required | Description |
|---|---|---|
| `limit` | — | Max indexes to return (default 20) |
| `min_neurons` | — | Filter indexes with at least this many neurons |

### Security

- `mcp.apiKey` → validated from `X-API-Key` or `Authorization: Bearer <key>`
- Empty `apiKey` → unauthenticated (development only)
- `mcp.allowedTools` → optional allowlist (empty = all tools enabled)

### Rate limiting

- Independent from REST rate limiter
- `mcp.rateLimitRPS` (default `30`) + `mcp.rateLimitBurst` (default `60`)
- Set to `0` to disable MCP-specific rate limiting

### MCP config example

```yaml
mcp:
  enabled: true
  path: /mcp
  apiKey: "your-secret-key"
  stateless: true
  rateLimitRPS: 30
  rateLimitBurst: 60
  enablePrompts: true
  allowedTools:
    - qubicdb_write
    - qubicdb_search
    - qubicdb_context
```

---

## 21) Benchmarks

### Benchmark suites in repository

| Suite | File | What it measures |
|---|---|---|
| Engine microbenchmarks | `pkg/engine/benchmark_test.go` | AddNeuron, Search, ParallelSearch, Levenshtein, tokenize, hybrid score |
| Worker/concurrency | `pkg/concurrency/benchmark_test.go` | BrainWorker submit, async submit, parallel submit |
| Live vector e2e | `pkg/e2e/vector_benchmark_test.go` | EmbedText (live model), hybrid search with live vectorizer |

### Repro commands

```bash
# Engine (lexical/hybrid path microbenchmarks)
go test -run '^$' -bench 'BenchmarkMatrixEngine(AddNeuron|Search|ParallelSearch)$' \
  -benchmem ./pkg/engine

# Worker/concurrency path
go test -run '^$' -bench 'BenchmarkBrainWorker(AddNeuron|Search|Parallel)$' \
  -benchmem ./pkg/concurrency

# Hybrid score microbenchmark (no model needed)
go test -run '^$' -bench 'BenchmarkSearcherHybrid' -benchmem ./pkg/engine

# Live vector benchmarks (requires model + libllama_go.dylib/.so)
QUBICDB_VECTOR_MODEL_PATH=./dist/MiniLM-L6-v2.Q8_0.gguf \
  go test -run '^$' -bench . -benchmem ./pkg/e2e
```

### Local benchmark snapshot

Environment: `goos=darwin`, `goarch=arm64`, CPU `Apple M3`

> Results are hardware/runtime dependent. Use for baseline direction, not universal SLA.

| Suite | Benchmark | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| engine | MatrixEngineAddNeuron | 704,893 | 603,635 | 15,019 |
| engine | MatrixEngineSearch (1K neurons) | 2,277,576 | 1,028,257 | 12,064 |
| engine | MatrixEngineParallelSearch | 414,734 | 698,656 | 10,037 |
| engine | SearcherHybridScoreVector384 | — | — | — |
| engine | SearcherHybridScan5K | — | — | — |
| concurrency | BrainWorkerAddNeuron | 1,394,905 | 1,220,190 | 20,783 |
| concurrency | BrainWorkerSearch | 1,759,222 | 1,389,654 | 15,262 |
| concurrency | BrainWorkerParallel (async) | **80.43** | 160 | 4 |

**Key observations:**
- Parallel async submit path is extremely fast (80 ns/op) — the serialized worker queue has minimal overhead
- Search at 1K neurons takes ~2.3ms — scales linearly with neuron count
- Parallel search benefits from read-lock concurrency (~5.5x faster than serial)

---

## 22) Capacity Planning & Tuning

### Memory footprint per index

Each `Neuron` in memory carries:
- Fixed fields: ~200 bytes (IDs, timestamps, energy, depth, access count)
- `content`: variable (up to `maxNeuronContentBytes` = 64 KB)
- `position`: `currentDim × 8 bytes` (float64 slice, default dim=3 → 24 bytes)
- `embedding`: `384 × 4 bytes = 1,536 bytes` (float32, when vector enabled)
- `metadata`: variable

**Rough estimate per neuron (vector enabled, typical content ~200 chars):**
~2–4 KB per neuron in memory

**For 1M neurons per index:** ~2–4 GB RAM per index (worst case)

### Disk footprint

`.nrdb` file = msgpack(Matrix) + optional gzip. With `compress=true`, expect 40–60% size reduction on typical text content.

**Rough estimate:** ~1–2 KB per neuron on disk (compressed)

### Tuning for high-throughput write workloads

```yaml
storage:
  fsyncPolicy: interval
  fsyncInterval: 5s       # Reduce fsync frequency
  walEnabled: true
daemons:
  persistInterval: 5m     # Batch writes less frequently
  decayInterval: 5m       # Reduce decay CPU overhead
```

### Tuning for low-latency search

```yaml
vector:
  alpha: 0.4              # Reduce vector weight (faster if model is slow)
  enabled: false          # Disable entirely for pure lexical speed
matrix:
  maxDimension: 100       # Smaller spatial positions = less memory
daemons:
  reorgInterval: 1h       # Reorg is expensive, run less often
```

### Tuning for memory-constrained environments

```yaml
matrix:
  maxNeurons: 10000       # Hard cap per index
  maxDimension: 50
lifecycle:
  dormantThreshold: 5m    # Evict workers faster
worker:
  maxIdleTime: 5m
daemons:
  pruneInterval: 2m       # Prune weak synapses more aggressively
```

### Tuning for maximum durability

```yaml
storage:
  fsyncPolicy: always     # fsync every write
  walEnabled: true
  startupRepair: true
  checksumValidationInterval: 1h  # Periodic integrity scan
```

### Index count scaling

- Each active index = one goroutine + one in-memory Matrix
- Workers idle beyond `worker.maxIdleTime` are evicted (matrix reloaded from disk on next access)
- For many low-traffic indexes: increase `lifecycle.dormantThreshold` and decrease `worker.maxIdleTime` to keep RAM usage bounded

### Vector model requirements

- **Model:** `MiniLM-L6-v2.Q8_0.gguf` (default) — ~90 MB on disk, ~100 MB RAM
- **Library:** `libllama_go.dylib` (macOS) / `libllama_go.so` (Linux) in standard lib paths
- **GPU offload:** set `vector.gpuLayers > 0` to offload layers to GPU (requires CUDA/Metal)
- **Graceful degradation:** if model or library not found, server starts normally with pure lexical search

---

## 23) Production Checklist

### Security
- [ ] Change `admin.password` from default `qubicdb`
- [ ] Set `security.allowedOrigins` to specific domain(s), not `*`
- [ ] Enable TLS: set `security.tlsCert` + `security.tlsKey`
- [ ] Set `QUBICDB_ENV=production` to enforce password validation
- [ ] Set `mcp.apiKey` if MCP is enabled
- [ ] Review `security.maxRequestBody` and `security.maxNeuronContentBytes` for your workload

### Persistence
- [ ] Set `storage.fsyncPolicy` to `always` or `interval` (never `off` in production)
- [ ] Enable `storage.walEnabled=true`
- [ ] Enable `storage.startupRepair=true`
- [ ] Set `storage.checksumValidationInterval` for periodic integrity checks (e.g. `24h`)
- [ ] Ensure `storage.dataPath` is on a durable volume with adequate space

### Operations
- [ ] Decide `registry.enabled` — use `true` for controlled multi-tenant deployments
- [ ] Set lifecycle thresholds appropriate for your traffic patterns
- [ ] Configure daemon intervals (avoid `decayInterval < 5s` or `persistInterval < 5s`)
- [ ] Set `matrix.maxNeurons` appropriate for expected index sizes
- [ ] Set `worker.maxIdleTime` to bound memory usage from idle indexes

### Vector search
- [ ] Verify `libllama_go.dylib/.so` is in library path before enabling vector search
- [ ] Download and place GGUF model at `vector.modelPath`
- [ ] Tune `vector.alpha` for your use case (default `0.6` is a good starting point)
- [ ] Consider `vector.gpuLayers > 0` for GPU acceleration

### Client integration
- [ ] Always branch on error `code`, not `error` message text
- [ ] Use `X-Index-ID` header (not query param) for index scoping
- [ ] Use `qubicdb://` connection strings for SDK/CLI configuration
- [ ] Handle `RATE_LIMITED` (429) with exponential backoff and `Retry-After` header
- [ ] Handle `MUTATION_DISABLED` (400) — do not call touch/forget/fire endpoints
