#!/bin/bash

INTERFACE="eth0"
CAPTURE_DIR="$HOME/safe-athens/pcap-capture/pcap-out"
DURATION=300
SNAPLEN=128
# FILTER='not port 22'
SERVER="john@192.168.2.8"
DEST="/home/john/safe-athens-server/pcap-system/inbox"
LOG="$HOME/safe-athens/pcap-capture/capture_send.log"
MIN_FREE_MB=500
RSYNC_RETRIES=3
RSYNC_RETRY_DELAY=10

mkdir -p "$CAPTURE_DIR"
exec >> "$LOG" 2>&1

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] ${*:2}"; }

cleanup() {
    log INFO "Shutting down (capture=$CAPTURE_PID send=$SEND_PID)"
    kill "$CAPTURE_PID" "$SEND_PID" 2>/dev/null
    wait "$CAPTURE_PID" "$SEND_PID" 2>/dev/null
    exit 0
}
trap cleanup SIGINT SIGTERM

capture_loop() {
    while true; do
        FREE_MB=$(df -m "$CAPTURE_DIR" | awk 'NR==2 {print $4}')
        if (( FREE_MB < MIN_FREE_MB )); then
            log WARN "Low disk space (${FREE_MB}MB free) — skipping capture, sleeping 60s"
            sleep 60
            continue
        fi

        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        HOST=$(hostname)
        TMPFILE="$CAPTURE_DIR/.node_${HOST}_${TIMESTAMP}.pcap.tmp"
        FILE="$CAPTURE_DIR/node_${HOST}_${TIMESTAMP}.pcap"

        log INFO "Starting capture -> $FILE"
        sudo timeout "$DURATION" tcpdump -i "$INTERFACE" -s "$SNAPLEN" "$FILTER" -w "$TMPFILE" 2>/dev/null
        STATUS=$?

        if [[ -f "$TMPFILE" ]]; then
            mv "$TMPFILE" "$FILE"
            # Background gzip — inotifywait close_write on .pcap.gz fires when done
            gzip "$FILE" &
            log INFO "Capture done (tcpdump exit $STATUS), compressing in background"
        else
            log ERROR "Capture produced no output (tcpdump exit $STATUS)"
        fi
    done
}

send_file() {
    local FILE="$1"
    local ATTEMPT=0
    while (( ATTEMPT < RSYNC_RETRIES )); do
        (( ATTEMPT++ ))
        rsync -av --remove-source-files "$CAPTURE_DIR/$FILE" "$SERVER:$DEST"
        if (( $? == 0 )); then
            log INFO "Sent $FILE (attempt $ATTEMPT)"
            return 0
        fi
        log WARN "rsync failed for $FILE (attempt $ATTEMPT/$RSYNC_RETRIES) — retrying in ${RSYNC_RETRY_DELAY}s"
        sleep "$RSYNC_RETRY_DELAY"
    done
    log ERROR "Giving up on $FILE after $RSYNC_RETRIES attempts — retained locally"
    return 1
}

send_loop() {
    # -m keeps inotifywait running continuously so no events are missed during rsync
    inotifywait -m -q -e close_write --format '%f' "$CAPTURE_DIR" | while read -r FILE; do
        [[ "$FILE" == *.pcap.gz ]] || continue
        log INFO "Detected $FILE — sending"
        send_file "$FILE"
    done
}

capture_loop &
CAPTURE_PID=$!

send_loop &
SEND_PID=$!

log INFO "Started — capture PID=$CAPTURE_PID, send PID=$SEND_PID"

wait
log ERROR "A subprocess exited unexpectedly — shutting down"
cleanup
