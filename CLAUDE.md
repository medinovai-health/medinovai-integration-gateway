# CLAUDE.md — medinovai-integration-gateway

> This file is read by every Claude agent at the start of each session.
> Keep it accurate. It is the agent's primary source of truth about this repo.

## Purpose

**medinovai-integration-gateway** is the Phase E core ingestion gateway for the MedinovAI Integration Platform. It receives clinical data from site-level EPG instances (mTLS) and external connectors (HTTPS), validates FHIR R4 and HL7v2 payloads, applies canonical transformation toward OMOP CDM v5.4, emits CloudEvents to the real-time stream bus, and coordinates routing toward OMOP lakehouse storage. Upstream teams and platform squads consume this service as the controlled ingress boundary between edge connectors and platform analytics.

## Compliance Tier

**Tier 2** — Platform / infrastructure (integration ingress; PHI in transit only per specification).

Applicable regulations: HIPAA (transit encryption, audit trail on ingestion), FDA 21 CFR Part 11 (immutable audit trail, electronic records integrity). Full Tier 1 PHI-at-rest controls are delegated upstream (EPG) per spec.

## Tech Stack

- Backend: Python 3.11, FastAPI
- Frontend: None
- Database: None in scaffold (readiness will eventually reflect EPG + datastore connectivity)
- Cache: None (scaffold)
- Messaging: CloudEvents / Kafka via `medinovai-real-time-stream-bus` (planned)
- Infrastructure: Docker / Kubernetes (reference Dockerfile and compose)
- Monitoring: Structured JSON logs (structlog, ZTA-aligned fields)

## How to Start the Dev Server

```bash
bash init.sh
```

Or manually:

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Dev server runs at: `http://localhost:8000`  
Health endpoint: `GET /health` → 200 OK  
Readiness: `GET /ready` → 200 OK (stub until dependency checks are implemented)

## How to Run Unit Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/ -q
```

Minimum coverage: 80% (target for production hardening; not enforced in Session 0 scaffold).

## How to Run End-to-End Tests

```bash
# After defining Playwright or HTTP integration tests under tests/integration/
pytest tests/integration/ -q
```

## Coding Conventions (MedinovAI Standard)

- Constants: `E_VARIABLE` (uppercase, `E_` prefix)
- Variables: `mos_variableName` (lowerCamelCase, `mos_` prefix)
- Methods: max 40 lines; split into helpers if longer
- Docstrings: Google style on public functions and classes
- Error handling: never swallow exceptions; log with correlation ID (structured, no PHI values)
- Secrets: AWS Secrets Manager / `medinovai-secrets-manager-bridge` only — never hardcode
- Orchestration: Claude Agent SDK, ActiveMQ, AWS Step Functions, Temporal (platform standard) — no n8n

## API Standards

- RESTful JSON APIs; OpenAPI via FastAPI
- Bearer authentication for protected routes (MSS / platform JWT — not implemented in scaffold)
- Rate limiting and gateway policies at edge (future)

## Tier 1 Compliance Requirements

**Not applicable** — this repo is **Tier 2**. Tier 1 PHI-at-rest and clinical-device traceability gates apply to downstream clinical repos. This service still implements audit-friendly logging and secure transit per the service specification.

## Git Branch Strategy

- `main`: production-ready only; no direct commits from agents
- `develop`: integration branch (if used)
- Feature branches: `feature/F###-short-description`
- Agents commit to feature branches and open PRs

## Known Issues / Current State

- Session 0 scaffold: `/health` and `/ready` only; ingest, batch, connectors, and mapping routes are not yet implemented.
- mTLS, Kafka/CloudEvents, and OMOP persistence are stubbed in `feature_list.json` for incremental delivery.

## Last Updated

2026-03-30 — Harness 2.1 Session 0 initializer (Tier 2 template, Section 9)

## Code Navigation — jCodeMunch (use instead of reading files)

All repos are pre-indexed by a background daemon. Use these MCP tools:

```
list_repos                                             → check indexed repos
search_symbols: { "repo": "<name>", "query": "..." }  → find functions/classes
get_symbol:     { "repo": "<name>", "symbol_id": "..." } → get exact source
get_repo_outline:   { "repo": "<name>" }               → repo structure
get_context_bundle: { "repo": "<name>", "symbol_id": "..." } → symbol + imports
```

Fall back to direct file reads only when editing. Zero cost — uses local Ollama.
