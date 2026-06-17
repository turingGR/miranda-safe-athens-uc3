#!/bin/bash

# pcap_merge_dedup_loop.sh
#
# Runs continuously on the server (safe-athens-server, 192.168.2.8).
# Every 60 seconds it picks up all .pcap and .pcap.gz files that the
# edge nodes (edge-01, edge-02) have deposited in inbox/ via rsync,
# merges them into a single chronologically sorted pcap, removes exact
# duplicate packets, and writes the result to merged/ for the Kafka
# producer (send_pcap_kafka.py) to forward to the EU partner.
#
# Pipeline per cycle:
#   inbox/ (.pcap / .pcap.gz)
#     → hash check   (skip files already processed in a previous cycle)
#     → decompress   (gunzip .pcap.gz into a temp workdir)
#     → mergecap     (combine all pcaps into one, sorted by timestamp)
#     → editcap -d   (remove duplicate packets within the merged file)
#     → merged/      (final output, named safe_athens_<timestamp>.pcap)
#     → archive/     (original inbox files moved here after success)

INBOX="$HOME/safe-athens-server/pcap-system/inbox"
MERGED="$HOME/safe-athens-server/pcap-system/merged"
ARCHIVE="$HOME/safe-athens-server/pcap-system/archive"
TMP="$HOME/safe-athens-server/pcap-system/tmp"
LOGFILE="$HOME/safe-athens-server/pcap-system/logs/merge.log"

# Persistent log of sha256 hashes of every file ever processed.
# Prevents re-processing a file if rsync delivers it twice
# (e.g. edge node crashed before --remove-source-files completed).
HASH_LOG="$HOME/safe-athens-server/pcap-system/logs/processed_hashes.log"

# Seconds to wait between processing cycles
INTERVAL=60

mkdir -p "$INBOX" "$MERGED" "$ARCHIVE" "$TMP" "$(dirname "$LOGFILE")"

while true
do
    # Collect all pcap files currently in inbox (plain and gzipped)
    PCAPS=("$INBOX"/*.pcap)
    GZPCAPS=("$INBOX"/*.pcap.gz)

    # Check if anything arrived — bash expands a non-matching glob to the
    # literal pattern string, so we test the first element with -e
    HAS_FILES=false
    [ -e "${PCAPS[0]}" ] && HAS_FILES=true
    [ -e "${GZPCAPS[0]}" ] && HAS_FILES=true

    if [ "$HAS_FILES" = false ]; then
        echo "$(date) - No pcap files found" >> "$LOGFILE"
        sleep "$INTERVAL"
        continue
    fi

    # Create a timestamped temp workdir for this cycle so parallel runs
    # (if ever triggered manually) don't collide
    TS=$(date +"%Y%m%d_%H%M%S")
    WORKDIR="$TMP/work_$TS"
    mkdir -p "$WORKDIR"

    echo "$(date) - Preparing files for merge" >> "$LOGFILE"

    # Copy plain pcaps into workdir as-is
    if [ -e "${PCAPS[0]}" ]; then
        cp "${PCAPS[@]}" "$WORKDIR"/
    fi

    # Decompress gzipped pcaps into workdir (keep originals in inbox for archiving)
    if [ -e "${GZPCAPS[0]}" ]; then
        for f in "${GZPCAPS[@]}"; do
            gunzip -c "$f" > "$WORKDIR/$(basename "${f%.gz}")"
        done
    fi

    MERGED_TMP="$TMP/merged_$TS.pcap"
    DEDUP_TMP="$TMP/dedup_$TS.pcap"
    OUTFILE="$MERGED/safe_athens_$TS.pcap"

    FILES_TO_MERGE=("$WORKDIR"/*.pcap)

    if [ ! -e "${FILES_TO_MERGE[0]}" ]; then
        echo "$(date) - No decompressed pcaps available for merge" >> "$LOGFILE"
        rm -rf "$WORKDIR"
        sleep "$INTERVAL"
        continue
    fi

    # Cross-cycle duplicate detection: compute sha256 of each inbox file and
    # skip it if we have seen that exact file before. This guards against rsync
    # delivering the same file twice when the edge node restarts unexpectedly.
    for f in "${PCAPS[@]}" "${GZPCAPS[@]}"; do
        [ -e "$f" ] || continue
        HASH=$(sha256sum "$f" | awk '{print $1}')
        if grep -qF "$HASH" "$HASH_LOG" 2>/dev/null; then
            echo "$(date) - Skipping duplicate file: $f" >> "$LOGFILE"
            mv "$f" "$ARCHIVE"/
        else
            echo "$HASH" >> "$HASH_LOG"
        fi
    done

    # Merge all pcaps from this cycle into one file, sorted by packet timestamp
    echo "$(date) - Merging ${#FILES_TO_MERGE[@]} files into $MERGED_TMP" >> "$LOGFILE"
    mergecap "${FILES_TO_MERGE[@]}" -w "$MERGED_TMP"

    if [ ! -f "$MERGED_TMP" ]; then
        echo "$(date) - Merge failed" >> "$LOGFILE"
        rm -rf "$WORKDIR"
        sleep "$INTERVAL"
        continue
    fi

    # Remove exact duplicate packets (same timestamp + same content) that can
    # arise when capture windows overlap between the two edge nodes
    editcap -d "$MERGED_TMP" "$DEDUP_TMP"

    if [ ! -f "$DEDUP_TMP" ]; then
        echo "$(date) - Dedup failed" >> "$LOGFILE"
        rm -f "$MERGED_TMP"
        rm -rf "$WORKDIR"
        sleep "$INTERVAL"
        continue
    fi

    # Promote the deduplicated file to the merged output directory
    mv "$DEDUP_TMP" "$OUTFILE"
    rm -f "$MERGED_TMP"
    rm -rf "$WORKDIR"

    echo "$(date) - Merge+Dedup success: $OUTFILE" >> "$LOGFILE"

    # Archive original inbox files so they are kept for reference but no longer
    # picked up in future cycles
    [ -e "${PCAPS[0]}" ] && mv "${PCAPS[@]}" "$ARCHIVE"/
    [ -e "${GZPCAPS[0]}" ] && mv "${GZPCAPS[@]}" "$ARCHIVE"/

    sleep "$INTERVAL"
done
