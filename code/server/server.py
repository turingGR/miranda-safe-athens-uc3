from flask import Flask, request, jsonify
from confluent_kafka import Producer
from datetime import datetime
import json
import threading

app = Flask(__name__)

producer = Producer({
    "bootstrap.servers": "192.168.2.8:9092",
    "client.id": "safe-athens-server"
})

latest_data = {}
lock = threading.Lock()


def gas_status(value):
    if value is None:
        return "unknown"
    if value < 150:
        return "good"
    if value < 400:
        return "warning"
    if value < 800:
        return "danger"
    return "critical"


def air_status(value):
    if value is None:
        return "unknown"
    if value < 50:
        return "excellent"
    if value < 150:
        return "normal"
    if value < 300:
        return "poor"
    return "hazardous"


def scale_percent(value, min_value=0, max_value=1023):
    if value is None:
        return None
    value = max(min_value, min(max_value, value))
    return round((value - min_value) / (max_value - min_value) * 100, 2)


def comfort_status(temp, hum):
    if temp is None or hum is None:
        return "unknown"
    if 20 <= temp <= 26 and 30 <= hum <= 60:
        return "comfortable"
    return "check"


def compute_risk(data):
    score = 0

    if data.get("motion") is True:
        score += 20

    gas = data.get("gas")
    air = data.get("air_quality")

    if gas is not None:
        if gas >= 800:
            score += 50
        elif gas >= 400:
            score += 30
        elif gas >= 150:
            score += 10

    if air is not None:
        if air >= 300:
            score += 40
        elif air >= 150:
            score += 20
        elif air >= 50:
            score += 5

    return min(score, 100)


def transform_payload(payload):
    transformed = dict(payload)
    transformed["gas_percent"] = scale_percent(payload.get("gas"))
    transformed["air_percent"] = scale_percent(payload.get("air_quality"))
    transformed["gas_status"] = gas_status(payload.get("gas"))
    transformed["air_status"] = air_status(payload.get("air_quality"))
    transformed["comfort_status"] = comfort_status(
        payload.get("temperature"),
        payload.get("humidity")
    )
    transformed["risk_score"] = compute_risk(payload)
    transformed["server_received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return transformed


def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA ERROR] {err}")
    else:
        print(f"[KAFKA OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


@app.route("/ingest", methods=["POST"])
def ingest():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    node_id = payload.get("node_id")
    if not node_id:
        return jsonify({"ok": False, "error": "missing node_id"}), 400

    transformed = transform_payload(payload)

    with lock:
        latest_data[node_id] = transformed

    producer.produce(
        "edge_raw",
        key=node_id,
        value=json.dumps(payload).encode("utf-8"),
        callback=delivery_report
    )

    producer.produce(
        "edge_overall",
        key=node_id,
        value=json.dumps(transformed).encode("utf-8"),
        callback=delivery_report
    )

    if transformed["risk_score"] >= 40:
        producer.produce(
            "edge_alerts",
            key=node_id,
            value=json.dumps(transformed).encode("utf-8"),
            callback=delivery_report
        )

    producer.poll(0)

    return jsonify({"ok": True, "node_id": node_id})


@app.route("/api/latest", methods=["GET"])
def api_latest():
    with lock:
        return jsonify(latest_data)


@app.route("/")
def home():
    return """
    <html>
    <head>
        <title>SAFE Athens Kafka Ingest Server</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f4f4f4; }
            .grid { display: flex; flex-wrap: wrap; gap: 20px; }
            .card {
                background: white;
                border: 1px solid #ccc;
                border-radius: 10px;
                padding: 16px;
                width: 420px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.08);
            }
            .title { font-size: 22px; font-weight: bold; margin-bottom: 12px; }
            .good { color: green; font-weight: bold; }
            .warn { color: orange; font-weight: bold; }
            .bad { color: red; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>SAFE Athens Central Dashboard</h1>
        <div id="grid" class="grid"></div>

        <script>
            function cls(status) {
                if (!status) return '';
                if (['good', 'excellent', 'comfortable', 'normal'].includes(status)) return 'good';
                if (['warning', 'poor', 'check'].includes(status)) return 'warn';
                if (['danger', 'critical', 'hazardous'].includes(status)) return 'bad';
                return '';
            }

            async function refresh() {
                const res = await fetch('/api/latest');
                const data = await res.json();

                const grid = document.getElementById('grid');
                grid.innerHTML = '';

                for (const [node, d] of Object.entries(data)) {
                    const card = document.createElement('div');
                    card.className = 'card';

                    card.innerHTML = `
                        <div class="title">${node}</div>
                        <div><b>IP:</b> ${d.ip ?? '-'}</div>
                        <div><b>Temperature:</b> ${d.temperature ?? '-'} °C</div>
                        <div><b>Humidity:</b> ${d.humidity ?? '-'} %</div>
                        <div><b>Pressure:</b> ${d.pressure ?? '-'} hPa</div>
                        <div><b>Motion:</b> ${d.motion}</div>
                        <div><b>Gas raw:</b> ${d.gas ?? '-'}</div>
                        <div><b>Gas %:</b> ${d.gas_percent ?? '-'}</div>
                        <div><b>Gas status:</b> <span class="${cls(d.gas_status)}">${d.gas_status ?? '-'}</span></div>
                        <div><b>Air raw:</b> ${d.air_quality ?? '-'}</div>
                        <div><b>Air %:</b> ${d.air_percent ?? '-'}</div>
                        <div><b>Air status:</b> <span class="${cls(d.air_status)}">${d.air_status ?? '-'}</span></div>
                        <div><b>Comfort:</b> <span class="${cls(d.comfort_status)}">${d.comfort_status ?? '-'}</span></div>
                        <div><b>Risk score:</b> ${d.risk_score ?? '-'} / 100</div>
                        <div><b>Edge timestamp:</b> ${d.timestamp ?? '-'}</div>
                        <div><b>Received:</b> ${d.server_received_at ?? '-'}</div>
                    `;

                    grid.appendChild(card);
                }
            }

            refresh();
            setInterval(refresh, 2000);
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
