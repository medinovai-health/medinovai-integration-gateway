# medinovai-integration-gateway

Phase E **Integration Platform** ingress service: accepts FHIR R4, HL7v2, and CSV from EPG (mTLS) and external connectors, transforms toward OMOP CDM v5.4, and emits CloudEvents to the platform stream bus.

## Specification

Canonical copy: `specs/active/medinovai-integration-gateway/specification.yaml` (synced from the platform brain repo).

## Quick start (local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

- Health: `GET http://localhost:8000/health`
- Ready: `GET http://localhost:8000/ready`
- Docs: `http://localhost:8000/docs`

## Harness

```bash
chmod +x init.sh   # once
./init.sh
```

## Docker

```bash
docker compose build
docker compose up
```

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
python3 -m pytest tests/ -q
```

If a globally installed pytest plugin fails to load (for example PIL architecture errors), run:

`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/ -q`

## Naming

- Constants: `E_*`
- Variables: `mos_*`

## Related platform services

- `medinovai-epg`, `medinovai-real-time-stream-bus`, `medinovai-data-services`, `medinovai-secrets-manager-bridge`, `medinovai-registry`
