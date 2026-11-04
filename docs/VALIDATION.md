# Validation record

Date: 14 October 2026 (Asia/Singapore).

## Automated component checks

`python -m pytest -q`: **57 passed** on the Windows Python 3.12 virtual environment.

Coverage includes certificate quorum/signatures, unique public-key membership, stale
authority and token handoff, concurrent grants/entries, expiry, missing updates,
session/phase/sequence freshness, delayed pre-entry clearance, restart, replay handling, clock rollback, commit failure,
missing/corrupt state, controller reconciliation, real gRPC health/metrics, mutual TLS,
and three deterministic 250-action sequences with an independent physical lane model.
A real subprocess console-signal test also verifies clean shutdown and database-lock
release. Windows uses a targeted Ctrl+Break event; Linux uses SIGINT. The server polls
its shutdown event every 200 ms to allow Windows console signals to be dispatched.

## Demonstration and packaging

- Local standalone real-gRPC demo: all 11 expected outcomes passed.
- Editable package installation: passed.
- Docker Compose configuration validation: passed.
- Docker image `cranelock-gate:0.1.0`: built successfully with Linux Python 3.12.
- Non-root container startup and health check: passed.
- Host client against the running container: all 11 demo outcomes passed.
- Container restart: retained epoch `1`, holder `crane-2`, revision `2`; invalidated
  outstanding grant; lane became UNKNOWN; the holder's new request was rejected
  with `LANE_UNKNOWN` until fresh confirmation.
- Post-restart demo: all 11 outcomes passed.

The image and initialised Compose volume are retained for reuse. Test containers are
stopped after verification. Development signing keys and the host database are in
the ignored `demo-data` directory; they are not production credentials.

## Remaining validation

Kubernetes deployment, real member-component integration, distributed voting discipline,
full-system physical safety traces, controller queue reconciliation, baseline comparisons
and scalability/performance measurements have not been validated. The supplied
Kubernetes manifest is a development template. These component results do not establish
end-to-end CraneLock safety or industrial suitability.
