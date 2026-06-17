#!/bin/bash

INTERFACE="eth0"
CAPTURE_DIR="$HOME/safe-athens/pcap-capture/pcap-out"
DURATION=300
SNAPLEN=128
# FILTER='not port 22'
MIN_FREE_MB=500

mkdir -p "$CAPTURE_DIR"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] ${*:2}"; }

# systemd kills the whole cgroup on stop, but log clean shutdown if signalled
trap 'log INFO "Shutting down"; exit' SIGTERM SIGINT

while true; do
    FREE_MB=$(df -m "$CAPTURE_DIR" | awk 'NR==2 {print $4}')
    if (( FREE_MB < MIN_FREE_MB )); then
        log WARN "Low disk space (${FREE_MB}MB free) — skipping, sleeping 60s"
        sleep 60
        continue
    fi

    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    HOST=$(hostname)
    TMPFILE="$CAPTURE_DIR/.node_${HOST}_${TIMESTAMP}.pcap.tmp"
    FILE="$CAPTURE_DIR/node_${HOST}_${TIMESTAMP}.pcap"

    log INFO "Starting capture -> $FILE"
    # Service runs as root — no sudo needed
    timeout "$DURATION" tcpdump -i "$INTERFACE" -s "$SNAPLEN" "$FILTER" -w "$TMPFILE" 2>/dev/null
    STATUS=$?

    if [[ -f "$TMPFILE" ]]; then
        mv "$TMPFILE" "$FILE"
        gzip "$FILE" &
        log INFO "Capture done (exit $STATUS), compressing in background"
    else
        log ERROR "Capture produced no output (tcpdump exit $STATUS)"
    fi
done
