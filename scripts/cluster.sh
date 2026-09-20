#!/usr/bin/env bash
# Spin up a local N-node gossip cluster, or tear it down.
#
#   ./scripts/cluster.sh up 5      # start 5 nodes
#   ./scripts/cluster.sh status    # the nodetool-status equivalent
#   ./scripts/cluster.sh kill 5    # SIGKILL node 5 and watch phi climb
#   ./scripts/cluster.sh probe     # measure end-to-end propagation latency
#   ./scripts/cluster.sh down
set -euo pipefail

BIN="${BIN:-./bin/gossipd}"
RUN_DIR="${RUN_DIR:-/tmp/gossip-go-cluster}"
# setsid detaches the child from this terminal's process group so it survives
# the script exiting. It is a util-linux tool and absent on macOS, where nohup
# plus a background job is already enough. `env` is the no-op stand-in: bash 3.2
# (still the macOS default) treats an empty array under `set -u` as unbound.
LAUNCHER=setsid; command -v setsid >/dev/null 2>&1 || LAUNCHER=env
BASE_UDP=7000
BASE_HTTP=8000
SEEDS="127.0.0.1:7001,127.0.0.1:7002"

up() {
  local n="${1:-5}"
  [[ -x "$BIN" ]] || { echo "build first: make build" >&2; exit 1; }
  mkdir -p "$RUN_DIR"
  for i in $(seq 1 "$n"); do
    local dc="DC1"
    [[ $i -gt 3 ]] && dc="DC2"
    nohup "$LAUNCHER" "$BIN" \
      -bind "127.0.0.1:$((BASE_UDP + i))" \
      -http "127.0.0.1:$((BASE_HTTP + i))" \
      -seeds "$SEEDS" \
      -dc "$dc" -rack RAC1 \
      > "$RUN_DIR/node$i.log" 2>&1 < /dev/null &
    printf 'node%-3s udp=127.0.0.1:%s  http=127.0.0.1:%s  dc=%s\n' \
      "$i" "$((BASE_UDP + i))" "$((BASE_HTTP + i))" "$dc"
  done
  echo
  echo "logs: $RUN_DIR/   next: ./scripts/cluster.sh status"
}

status() {
  local port="${1:-$((BASE_HTTP + 1))}"
  # NB: python3 -c, not a heredoc. A heredoc binds stdin and would silently
  # discard the piped JSON.
  local body
  if ! body=$(curl -sf --max-time 2 "127.0.0.1:${port}/members"); then
    echo "no node answering on 127.0.0.1:${port} — is the cluster up? (make up)" >&2
    return 1
  fi
  printf '%s' "$body" | python3 -c '
import json, sys
rows = json.load(sys.stdin)
print("  %-22s %-6s %7s %13s %8s  %s" % ("ADDRESS","STATE","PHI","GENERATION","VERSION","DC"))
for m in rows:
    print("%s %-22s %-6s %7s %13s %8s  %s" % (
        "*" if m["self"] else " ", m["endpoint"],
        "UN" if m["live"] else "DN", m["phi"], m["generation"],
        m["version"], m["app_state"].get("DC", "-")))
'
}

kill_node() {
  local i="$1"
  pkill -KILL -f "bind 127.0.0.1:$((BASE_UDP + i))" && echo "SIGKILLed node${i}"
}

# probe writes a unique value on node1 and polls the others until they all have
# it. This is the propagation-latency measurement, not a simulation of one.
probe() {
  python3 - <<'PY'
import json, time, urllib.request
marker = "probe-%d" % int(time.time() * 1000)
req = urllib.request.Request("http://127.0.0.1:8001/state/PROBE",
                             data=marker.encode(), method="PUT")
t0 = time.time()
urllib.request.urlopen(req, timeout=2)

# Discover which HTTP ports actually answer, rather than assuming a size.
watchers = []
for p in range(8002, 8021):
    try:
        urllib.request.urlopen("http://127.0.0.1:%d/healthz" % p, timeout=0.3).read()
        watchers.append(p)
    except Exception:
        pass
seen = {}
while len(seen) < len(watchers) and time.time() - t0 < 30:
    for p in watchers:
        if p in seen:
            continue
        try:
            with urllib.request.urlopen("http://127.0.0.1:%d/members" % p, timeout=2) as r:
                for m in json.load(r):
                    if m["endpoint"] == "127.0.0.1:7001" and m["app_state"].get("PROBE") == marker:
                        seen[p] = time.time() - t0
        except Exception:
            pass
    time.sleep(0.02)

for p in watchers:
    print("  node%d saw it after %.2fs" % (p - 8000, seen.get(p, float("nan"))))
if seen:
    print("  full propagation: %.2fs" % max(seen.values()))
PY
}

down() {
  pkill -f "gossipd -bind 127.0.0.1:70" 2>/dev/null || true
  echo "cluster stopped"
}

case "${1:-}" in
  up)     shift; up "$@" ;;
  status) shift; status "$@" ;;
  kill)   shift; kill_node "$@" ;;
  probe)  probe ;;
  down)   down ;;
  *) echo "usage: $0 {up [n]|status [http-port]|kill <n>|probe|down}" >&2; exit 1 ;;
esac
