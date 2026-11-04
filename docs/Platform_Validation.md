# Platform validation

Date: 21 October 2026
Environment: macOS, Python 3.12.14

- Automated tests: 64 passed.
- Gate Docker image: built successfully.
- Container demo: all 11 expected outcomes observed.
- Container health after restart: healthy.
- Restart preserved epoch 3 and holder crane-2.
- Restart changed session, invalidated the grant and marked the lane UNKNOWN.

Evidence: gate-events.jsonl and gate-events-after-restart.jsonl.

Remaining: crane-agent containers, multi-service deployment,
Kubernetes, monitoring stack and automated failure injection.

## Containerised crane agents

- Built cranelock-agent:0.1.0.
- Started three agents with separate persistent volumes.
- Verified peer heartbeat communication.
- Verified token transfer from crane-a to crane-b.
- Gracefully stopped crane-b while it held the token.
- crane-a suspected crane-b approximately 2.32 seconds after
  its last heartbeat.
- crane-a retained epoch 1 and did not create a replacement token.
- Majority recovery and restart reconciliation remain unverified.