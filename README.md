# SAFE Athens — Παράδοση Pilot (κώδικας + επισκόπηση)

Φάκελος παράδοσης προς συνεργάτη του έργου **MIRANDA CDT** (WP5 / UC3 — Safe Athens).
Περιέχει τον πλήρη πηγαίο κώδικα του pilot και από τα τρία μηχανήματα, μαζί με μια
συνοπτική επισκόπηση (λειτουργικά συστήματα, αρχιτεκτονική κλπ).

## Περιεχόμενα

- **`SAFE_Athens_Pilot_Review_EL.docx`** — Η επισκόπηση του pilot (στα Ελληνικά). Ξεκινήστε από εδώ.
- **`code/server/`** — `server.py` (Flask ingest API, confluent-kafka), `requirements.txt`,
  `start.sh` (tmux), `pcap-system/scripts/` (`pcap_merge_dedup_loop.sh`, `send_pcap_kafka.py`),
  `kafka/config/safe-athens-server.properties` (KRaft).
- **`code/edge-01/`** & **`code/edge-02/`** — `client/stream.py` (αισθητήρες + MJPEG + POST /ingest),
  `pcap-capture/*.sh` (tcpdump capture & rsync send), `systemd/*.service`.

## Μηχανήματα (συνοπτικά)

| Κόμβος | IP | OS / Kernel | CPU |
|---|---|---|---|
| safe-athens-server | 192.168.2.8 | Ubuntu 24.04.4 LTS · 6.8.0-110 · x86_64 | Intel i5-12400 |
| safe-athens-edge-01 | 192.168.2.9 | Raspberry Pi OS (Debian 13 trixie) · 6.12.62-rpi · aarch64 | Cortex-A76 (4 πυρήνες) |
| safe-athens-edge-02 | 192.168.2.12 | Raspberry Pi OS (Debian 13 trixie) · 6.12.62-rpi · aarch64 | Cortex-A76 (4 πυρήνες) |

Python: 3.13.5 (edge), 3.12.3 (server).

## Kafka (data bus για τον consumer)

Broker `192.168.2.8:9092` — PLAINTEXT (χωρίς auth/TLS), 4 topics, 3 partitions, RF=1.

| Topic | Format | Key | Producer |
|---|---|---|---|
| `edge_raw` | JSON | `node_id` | server.py |
| `edge_overall` | JSON | `node_id` | server.py |
| `edge_alerts` | JSON | `node_id` | server.py (risk_score ≥ 40) |
| `safe-athens-pcap` | Binary (512 KB chunks) | `file_id` | send_pcap_kafka.py |

`safe-athens-pcap`: επανασυναρμολόγηση κατά `file_id`, σειρά κατά `chunk_index`, ολοκλήρωση στα `total_chunks` (headers: `file_id`, `filename`, `chunk_index`, `total_chunks`).

## Σημείωση

Τα δύο edge δεν τρέχουν ταυτόσημο `stream.py`: ο **edge-01** διαβάζει μόνο ένα αναλογικό
κανάλι (A0) ως `air_quality` και δεν στέλνει `gas`· ο **edge-02** στέλνει `gas` (A0) και
`air_quality` (A2). Λεπτομέρειες και λοιπές αποκλίσεις από την τεκμηρίωση: §7 του .docx.
