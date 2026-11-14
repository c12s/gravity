# gravity — c12s agent queue

The control plane's delivery service: a gRPC endpoint (`AgentQueue`) that
turns operator/control-plane requests into NATS messages addressed to
individual nodes. Every command that must reach a specific node passes
through here — config delivery, cluster join, app-config delivery, and OS
image updates.

> The Go module is still named `github.com/c12s/agent_queue` for historical
> reasons; the repository and the deployed service are `gravity`.

## Role in c12s

| Flow | Path |
|---|---|
| Config placement | Kuiper → `DeseminateConfig` → `<node>.configs` (+ webhook reply) |
| Cluster join | Magnetar `ClaimOwnership` → `JoinCluster` → `<node>.join` |
| App config | Meridian → `DisseminateAppConfig` → `<node>.app_config` |
| OS update | cockpit → gateway → **`DisseminateOSImage`** → Magnetar intent + `<node>.os_update` (sync ack ≤10 s) |

`DisseminateOSImage` is the OS-update entry point of the whole system.

## API (`pkg/api/proto/agent_queue.proto`, reflection enabled)

| RPC | Authorization | Behavior |
|---|---|---|
| `DeseminateConfig` | `config.send\|node\|<id>` | request/reply over `<node>.configs`; node replies are POSTed to the caller's webhook; subscription dropped after 10 s |
| `DisseminateAppConfig` | `config.app.send\|node\|<id>` | fire-and-forget publish to `<node>.app_config` |
| `JoinCluster` | **none** (see bugs) | publish `<address>|<clusterId>` to `<node>.join` |
| `DisseminateOSImage` | `os.update` on the node, or on any org (pre-check; Magnetar then checks the node's own org and is authoritative) | validate → record intent in Magnetar (caller's token forwarded) → publish `<node>.os_update`, wait up to 10 s for the node's ack; returns `accepted: true/false` |

`DisseminateOSImage` validation (`internal/servers/agent_queue.go`): both
`image_ref` and `digest` required; `image_ref` must not embed a digest; digest
must match `sha256:<64 lowercase hex>`; `reboot_policy` must be empty or
`immediate`. Invalid specs are rejected **before** any ack, so a node never
enters a failing switch loop (review R3). Intent is recorded in Magnetar
*before* the NATS push: a node that misses the push still converges on its
next pull (DEV-14).

## Transport reliability

- The NATS connection uses `magnetarapi.NatsOptions()`: retry the first
  connect, reconnect forever. A gravity started while NATS is down exits
  rather than running deaf (the compose healthcheck handles restart).
- Reply handlers skip NATS no-responders/status-only messages instead of
  treating them as an empty reply (DEV-13).
- The no-ack case (`accepted: false`) is not an error for OS updates: the
  intent is already recorded, so an offline node converges later.

## Configuration

| Variable | Purpose |
|---|---|
| `NATS_CONN_ADDRESS` | NATS server (host:port), required |
| `GRPC_PORT` | gRPC listen port (lab: 50052), required |
| `SECRET_KEY` | JWT key for the `permissions` claim check |
| `MAGNETAR_ADDRESS` | Magnetar gRPC address (intent recording) |

## Build, test, run

```bash
go build ./...
go test ./...        # OS image validation tests (internal/servers/os_image_test.go)
go run ./cmd/server  # needs NATS_CONN_ADDRESS, GRPC_PORT; MAGNETAR_ADDRESS for OS updates
```

In compose (`tools/docker-compose.yml`) it runs as the `agent_queue`
container; `tools/.env` maps `GRAVITY_ADDRESS=agent_queue:50052`.

## Status

Stable for the thesis delivery paths: all config delivery and every OS
update (E1–E7) go through this service; validation and recorded-intent
behavior (review R3, DEV-14) are exercised by the experiments.

## Bugs, disabled and unfinished features

### Confirmed bugs

| Where | Effect |
|---|---|
| `JoinCluster` has **no authorization check** | Any caller able to reach the gRPC port can make any node join an arbitrary Serf cluster (`<node>.join` for any UUID). The old analysis flagged this as high; unchanged so far. |
| `DeseminateConfig` webhook handling | The target URL is fully caller-controlled (SSRF/exfiltration surface), no size limits, and the 10 s cleanup goroutine is not tied to the RPC context (review: medium, unchanged). |
| Ack semantics | The node ack (`OSUpdateAck`) carries no accepted/rejected reason; an invalid command at the node produces no reply at all, so gravity reports `accepted:false` exactly like an offline node (review R4). |

### Disabled / unfinished features

- **No per-node delivery guarantee for `JoinCluster` and
  `DisseminateAppConfig`** — both are fire-and-forget publishes; failures
  surface only in logs.
- JWT validation does not restrict the signing algorithm and relies on
  exact-string permission matching (pre-existing; documented in the original
  analysis).
- No integration tests: only the OS validation unit tests exist.

### PoC boundaries (by design, not bugs)

- Lab transport is plaintext (gRPC, NATS, webhooks); NATS itself is
  unauthenticated (review S1).
- NATS subjects are constructed from validated UUIDs only; payload contents
  are not otherwise inspected.

References: `../plan/deviations.md` (DEV-1, DEV-13, DEV-14),
`../plan/review.md` (R3, R4, S1), `../plan/architecture.md` §4.2.
