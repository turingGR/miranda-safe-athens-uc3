#!/bin/bash

# Starts all safe-athens-server services in a tmux session.
# Each window shows a live terminal.
# Usage:        ./start.sh
# Reattach:     tmux attach -t safe-athens
# Switch panes: Ctrl+b then 0-3 or arrow keys

SESSION="safe-athens"

# Kill existing session if running
tmux kill-session -t "$SESSION" 2>/dev/null

# Window 0: Kafka broker
tmux new-session -d -s "$SESSION" -n "kafka" \
    "/home/john/safe-athens-server/kafka/bin/kafka-server-start.sh \
     /home/john/safe-athens-server/kafka/config/safe-athens-server.properties"

# Wait for Kafka to be ready before starting the producer
sleep 8

# Window 1: server.py
tmux new-window -t "$SESSION" -n "server"
tmux send-keys -t "$SESSION:server" \
    "cd /home/john/safe-athens-server/server && source venv/bin/activate && python3 server.py" Enter

# Window 2: pcap merge/dedup loop
tmux new-window -t "$SESSION" -n "pcap-merge"
tmux send-keys -t "$SESSION:pcap-merge" \
    "bash /home/john/safe-athens-server/pcap-system/scripts/pcap_merge_dedup_loop.sh" Enter

# Window 3: Kafka pcap producer
tmux new-window -t "$SESSION" -n "kafka-producer"
tmux send-keys -t "$SESSION:kafka-producer" \
    "cd /home/john/safe-athens-server/pcap-system/scripts && source venv/bin/activate && python3 send_pcap_kafka.py" Enter

# Start on kafka window
tmux select-window -t "$SESSION:kafka"
tmux attach-session -t "$SESSION"
