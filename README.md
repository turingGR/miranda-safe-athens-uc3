# SAFE Athens — MIRANDA CDT (WP5 / UC3)

A real-time IoT edge-to-server pilot. Two Raspberry Pi 5 edge nodes collect environmental sensor data and network traffic; a central server processes them and streams everything through Apache Kafka for downstream analysis and ML-based intrusion detection.

## Repository layout

- **`code/server/`** — `server.py` (Flask ingest API, confluent-kafka), `requirements.txt`, `start.sh` (tmux), `pcap-system/scripts/` (`pcap_merge_dedup_loop.sh`, `send_pcap_kafka.py`), `kafka/config/safe-athens-server.properties` (KRaft).
- **`code/edge-01/`** & **`code/edge-02/`** — `client/stream.py` (sensors + MJPEG + POST /ingest), `pcap-capture/*.sh` (tcpdump capture & rsync send), `systemd/*.service`.

## Machines

| Node | OS / Kernel | CPU |
|---|---|---|
| server | Ubuntu 24.04.4 LTS · kernel 6.8.0-110 · x86_64 | Intel i5-12400 |
| edge-01 | Raspberry Pi OS (Debian 13 trixie) · kernel 6.12.62-rpi · aarch64 | Cortex-A76 (4 cores) |
| edge-02 | Raspberry Pi OS (Debian 13 trixie) · kernel 6.12.62-rpi · aarch64 | Cortex-A76 (4 cores) |

Python: 3.13.5 (edge), 3.12.3 (server).

## Kafka (data bus)

Single-node Apache Kafka 4.2.0 in KRaft mode, PLAINTEXT on port 9092, 4 topics, 3 partitions each, replication factor 1.

| Topic | Format | Key | Producer |
|---|---|---|---|
| `edge_raw` | JSON | `node_id` | server.py |
| `edge_overall` | JSON | `node_id` | server.py |
| `edge_alerts` | JSON | `node_id` | server.py (risk_score ≥ 40) |
| `safe-athens-pcap` | Binary (512 KB chunks) | `file_id` | send_pcap_kafka.py |

`safe-athens-pcap`: reassemble by `file_id`, order by `chunk_index`, complete at `total_chunks` (message headers: `file_id`, `filename`, `chunk_index`, `total_chunks`).

## Note

The two edge nodes do not run an identical `stream.py`: **edge-01** reads only one analog channel (A0) as `air_quality` and does not send a `gas` field, while **edge-02** sends both `gas` (A0) and `air_quality` (A2).
