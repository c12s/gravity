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
| `JoinCluster` | **none** | publish `<address>|<clusterId>` to `<node>.join` |
| `DisseminateOSImage` | `os.update` on the node, or on any org (pre-check; Magnetar then checks the node's own org and is authoritative) | validate → record intent in Magnetar (caller's token forwarded) → publish `<node>.os_update`, wait up to 10 s for the node's ack; returns `accepted: true/false` |

`DisseminateOSImage` validation (`internal/servers/agent_queue.go`): both
`image_ref` and `digest` required; `image_ref` must not embed a digest; digest
must match `sha256:<64 lowercase hex>`; `reboot_policy` must be empty or
`immediate`. Invalid specs are rejected **before** any ack, so a node never
enters a failing switch loop. Intent is recorded in Magnetar
*before* the NATS push: a node that misses the push still converges on its
next pull.

## Transport reliability

- The NATS connection uses `magnetarapi.NatsOptions()`: retry the first
  connect, reconnect forever. A gravity started while NATS is down exits
  rather than running deaf (the compose healthcheck handles restart).
- Reply handlers skip NATS no-responders/status-only messages instead of
  treating them as an empty reply.
- The no-ack case (`accepted: false`) is not an error for OS updates: the
  intent is already recorded, so an offline node converges later.

## Configuration

| Variable | Purpose |
|---|---|
| `NATS_CONN_ADDRESS` | NATS server (host:port), required |
| `GRPC_PORT` | gRPC listen port (e.g. 50052), required |
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
