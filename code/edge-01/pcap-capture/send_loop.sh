#!/bin/bash

CAPTURE_DIR="$HOME/safe-athens/pcap-capture/pcap-out"
SERVER="john@192.168.2.8"
DEST="/home/john/safe-athens-server/pcap-system/inbox"
RSYNC_RETRIES=3
RSYNC_RETRY_DELAY=10

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] ${*:2}"; }

trap 'log INFO "Shutting down"; exit' SIGTERM SIGINT

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

log INFO "Watching $CAPTURE_DIR"

# -m keeps inotifywait running continuously so no events are missed during rsync
inotifywait -m -q -e close_write --format '%f' "$CAPTURE_DIR" | while read -r FILE; do
    [[ "$FILE" == *.pcap.gz ]] || continue
    log INFO "Detected $FILE — sending"
    send_file "$FILE"
done
