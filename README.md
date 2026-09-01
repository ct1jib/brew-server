# brew-server

Experimental Rust Brew core for linking two or more MidnightBlue BlueStation or Flowstation TETRA base stations.

Reference spec from https://wiki.tetrapack.online/tetra/specifications/brew/

Version 0.2 adds:

- HTTP Digest authentication compatible with BlueStation's current WebSocket transport (MD5 + qop=auth).
- Single-use authenticated WebSocket session URLs returned by the discovery GET.
- Subscriber registration and talkgroup affiliation routing.
- Group speech routing with priority-based floor pre-emption.
- SDS routing using `SHORT_TRANSFER` + `SDS_TRANSFER`, and reverse `SDS_REPORT` delivery.
- Experimental private/simplex call routing for Brew call states 4..13.

## Important compatibility note

The current BlueStation source defines private/simplex state constants, but its Brew parser keeps most of those payloads as raw bytes and its worker currently exposes group voice/SDS commands rather than private-call commands. This server therefore treats private `SETUP_REQUEST` payloads conservatively: the first two little-endian `u32` values are interpreted as source ISSI and destination ISSI, and subsequent control/traffic packets are routed by UUID. Validate this against captures/specification before production use.

## Build and run

```bash
cargo run --release -- brew-server.toml
```

or:

```bash
docker compose up --build
```

Health check:

```bash
curl http://127.0.0.1:9000/healthz
```

## Configuration

`brew-server.toml`:

```toml
listen = "0.0.0.0:9000"
websocket_path = "/brew/"
websocket_subprotocol = "brew"
route_without_affiliations = true
allow_multiple_calls_per_group = true
higher_priority_number_wins = true
preempt_cause = 1

[tls]
enabled = false
cert_path = "/etc/brew-server/tls/cert.pem"
key_path = "/etc/brew-server/tls/key.pem"

[auth]
enabled = true
realm = "brew-server"
session_ttl_seconds = 300

[auth.users]
"100000001" = "change-me-bs1"
"100000002" = "change-me-bs2"
```

Use a different username/password for each BlueStation. The username is only an HTTP Digest identity; it does not have to equal a radio ISSI, although using a numeric site identity is convenient.

## TLS

Brew can terminate TLS natively so BlueStations connect over `wss://` / `https://` without a reverse proxy. Enable it in `[tls]`:

```toml
[tls]
enabled = true
cert_path = "/etc/brew-server/tls/cert.pem"
key_path = "/etc/brew-server/tls/key.pem"
```

`cert_path` is a PEM certificate chain (leaf first, then any intermediates) and `key_path` is the matching PEM private key (PKCS#8 or RSA). When `enabled = true` the listener serves HTTPS on the same `listen` address, so clients, the discovery GET, session WebSockets, and the dashboard all move to `https://`/`wss://`.

Generate a self-signed cert for lab use:

```bash
mkdir -p tls
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout tls/key.pem -out tls/cert.pem -subj "/CN=brew-server"
```

For Docker, mount the certs (the provided `docker-compose.yml` mounts `./tls` to `/etc/brew-server/tls`) and set the paths accordingly.

## BlueStation side

Configure each BlueStation's Brew transport to point at the server host/port, use endpoint `/brew` (or `/brew/`), subprotocol `brew`, and set the matching Digest username/password. With Digest credentials configured, current BlueStation performs:

1. `GET /brew/` without credentials.
2. Server returns `401` with a Digest challenge.
3. BlueStation retries with `Authorization: Digest ...`.
4. Server returns a one-time path such as `/brew/session/<token>`.
5. BlueStation upgrades that path to WebSocket with subprotocol `brew`.

## SDS routing

BlueStation sends SDS as two Brew packets with the same UUID:

```text
CALL_SHORT_TRANSFER(uuid, source ISSI, destination ISSI)
FRAME_SDS_TRANSFER(uuid, payload)
```

The server resolves the destination to the BlueStation currently owning that ISSI, forwards both packets, then routes `FRAME_SDS_REPORT(uuid, status)` back to the originating BlueStation. If the destination number is a currently affiliated GSSI instead, the SDS is multicast to the affiliated cells and reports are returned until the route expires.

SDS transaction state expires after 60 seconds.

## Group priority / pre-emption

By default a higher numeric priority wins (`higher_priority_number_wins = true`). If TG 91 currently has priority 3 and another cell starts TG 91 at priority 7, the server:

1. Generates `GROUP_IDLE` for the displaced call UUID using `preempt_cause`.
2. Sends it to the old transmitting cell and all routed listening cells.
3. Removes the old call/floor state.
4. Installs and forwards the new higher-priority call.

Equal/lower priority attempts are rejected while the floor is occupied. Set `higher_priority_number_wins = false` if your deployed Brew/TETRA profile uses inverse priority ordering.

## Private/simplex routing (experimental)

The following Brew call states are recognized and routed by call UUID:

- 4 SETUP_REQUEST
- 5 SETUP_ACCEPT
- 6 SETUP_REJECT
- 7 CALL_ALERT
- 8 CONNECT_REQUEST
- 9 CONNECT_CONFIRM
- 10 CALL_RELEASE
- 12 SIMPLEX_GRANTED
- 13 SIMPLEX_IDLE

`SETUP_REQUEST` establishes the route from the first 8 payload bytes (`source_issi:u32 LE`, `destination_issi:u32 LE`). The destination must currently be registered on another BlueStation. Thereafter control messages and traffic-channel frames may flow in either direction between the two participating cells until `CALL_RELEASE`.

Because current upstream BlueStation does not yet expose a complete private-call Brew command path, this feature should be considered server-ready/experimental rather than end-to-end validated.

## Scope and security

This is a lab/experimental core, not a production TETRA SwMI. Digest authentication protects credentials from being sent directly but MD5 Digest is legacy authentication; enable the built-in `[tls]` support (or deploy behind a TLS-terminating proxy) or run on a trusted private network. The server currently has no persistent subscriber database, ACL policy, rate limiting, or HA state replication.

## BlueStation connected/registered but no inter-BS calls

A subscriber `REGISTER` is not the same thing as a talk-group `AFFILIATE`. If the
server log contains `subscriber registered` but no `subscriber affiliated ... gssi=...`,
there is no affiliation table to route by. v0.2.1 therefore defaults
`fallback_broadcast_when_no_affiliations = true`: when a `GROUP_TX` arrives for a
GSSI with no recorded affiliations, it is sent to every other connected BlueStation.
Once `AFFILIATE` messages are present, selective GSSI routing is used again.

If pressing PTT still produces no `routed GROUP_TX` line at the server, the problem is
upstream of the server: BlueStation has not emitted the Brew `GROUP_TX`. Enable DEBUG
logging for BlueStation's Brew entity/worker and look for `forwarding local call to
TetraPack` / `sent GROUP_TX`. SDS also requires BlueStation's Brew SDS feature to be
enabled; otherwise BlueStation intentionally ignores `SendSds`.

## Web monitoring dashboard (v1)

This build includes a zero-setup live dashboard on the same HTTP listener as Brew.

- Dashboard: `http://<server>:9000/`
- JSON snapshot: `/api/status`
- Live event WebSocket: `/api/live`
- Existing Brew endpoint remains unchanged (normally `/brew`).

The dashboard shows connected BlueStations, registered subscribers, groups, active and
recent group/private calls, call durations/voice-frame counts, and recent SDS traffic.
Counters/history are currently in-memory and reset when the server restarts.

True RF TS1-TS4 utilization is intentionally not guessed from Brew traffic; that is the
next phase and requires a small telemetry feed from BlueStation.
