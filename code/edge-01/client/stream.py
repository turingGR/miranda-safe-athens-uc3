from flask import Flask, Response, jsonify
from picamera2 import Picamera2
from gpiozero import MotionSensor
import cv2
import time
import smbus2
import bme280
from grove.adc import ADC
import traceback
import requests
import threading

app = Flask(__name__)

# =========================
# SETTINGS
# =========================
PIR_GPIO = 5
BME280_ADDR = 0x76

FRAME_WIDTH = 640
FRAME_HEIGHT = 480

NODE_ID = "edge-01"
NODE_IP = "192.168.2.9"
SERVER_INGEST_URL = "http://192.168.2.8:5000/ingest"

# =========================
# CAMERA SETUP
# =========================
picam2 = Picamera2()
camera_config = picam2.create_video_configuration(
    main={"size": (FRAME_WIDTH, FRAME_HEIGHT), "format": "RGB888"}
)
picam2.configure(camera_config)
picam2.start()
time.sleep(2)

# =========================
# PIR SETUP
# =========================
pir = MotionSensor(PIR_GPIO)

# =========================
# BME280 SETUP
# =========================
bus = smbus2.SMBus(1)
calibration_params = bme280.load_calibration_params(bus, BME280_ADDR)

# --- Air Quality Setup
adc = ADC()
AIR_PORT = 0   # A0

def read_bme280():
    try:
        data = bme280.sample(bus, BME280_ADDR, calibration_params)
        return {
            "temperature": round(data.temperature, 2),
            "humidity": round(data.humidity, 2),
            "pressure": round(data.pressure, 2),
        }
    except Exception as e:
        print("BME280 error:", e)
        return {
            "temperature": None,
            "humidity": None,
            "pressure": None,
        }


def read_pir():
    try:
        return {"motion": pir.motion_detected}
    except Exception as e:
        print("PIR error:", e)
        return {"motion": None}


def get_sensor_data():
    result = {}
    result.update(read_bme280())
    result.update(read_pir())
    result.update(read_air())
    result["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return result


def read_air():
    try:
        return {"air_quality": adc.read(AIR_PORT)}
    except Exception as e:
        print("Air quality read error:", e)
        return {"air_quality": None}


def generate_frames():
    while True:
        try:
            frame = picam2.capture_array()

            # Convert RGB -> BGR for OpenCV drawing/encoding
            frame_bgr = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

            sensor_data = get_sensor_data()

            t = sensor_data["temperature"]
            h = sensor_data["humidity"]
            p = sensor_data["pressure"]
            m = sensor_data["motion"]
            a = sensor_data["air_quality"]            
            ts = sensor_data["timestamp"]

            line1 = f"Temp: {t if t is not None else 'N/A'} C"
            line2 = f"Hum: {h if h is not None else 'N/A'} %"
            line3 = f"Press: {p if p is not None else 'N/A'} hPa"
            line4 = f"Motion: {'YES' if m else 'NO' if m is not None else 'N/A'}"
            line5 = f"Air: {a if a is not None else 'N/A'}"            
            line6 = ts

            cv2.putText(frame_bgr, line1, (10, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 255, 0), 2)
            cv2.putText(frame_bgr, line2, (10, 55), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 255, 0), 2)
            cv2.putText(frame_bgr, line3, (10, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 255, 0), 2)
            cv2.putText(frame_bgr, line4, (10, 115), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 255, 255), 2)
            cv2.putText(frame_bgr, line5, (10, 145), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 0), 1)
            cv2.putText(frame_bgr, line6, (10, 175), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (255, 255, 255), 2)

            ret, buffer = cv2.imencode(".jpg", frame_bgr)
            if not ret:
                continue

            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" +
                buffer.tobytes() +
                b"\r\n"
            )

            time.sleep(0.03)

        except Exception as e:
            print("Frame generator error:", e)
            traceback.print_exc()
            time.sleep(1)

def sender_loop():
    while True:
        try:
            payload = get_sensor_data()
            payload["node_id"] = NODE_ID
            payload["ip"] = NODE_IP

            r = requests.post(SERVER_INGEST_URL, json=payload, timeout=2)
            print(f"[INGEST] {r.status_code} -> {payload}")
        except Exception as e:
            print("[INGEST ERROR]", e)

        time.sleep(2)


@app.route("/")
def index():
    return """
    <html>
    <head>
        <title>Camera + Grove Sensors</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
                background: #f7f7f7;
            }
            h1 {
                margin-bottom: 20px;
            }
            .container {
                display: flex;
                gap: 24px;
                align-items: flex-start;
                flex-wrap: wrap;
            }
            .panel {
                background: white;
                border: 1px solid #ccc;
                border-radius: 10px;
                padding: 15px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.08);
            }
            .panel h2 {
                margin-top: 0;
            }
            .sensor-row {
                margin: 10px 0;
                font-size: 18px;
            }
            img {
                border: 1px solid #ccc;
                border-radius: 10px;
                background: #000;
            }
        </style>
    </head>
    <body>
        <h1>Raspberry Pi Camera + Grove Sensors</h1>

        <div class="container">
            <div class="panel">
                <img src="/video_feed" width="640" height="480">
            </div>

            <div class="panel">
                <h2>Sensor Data</h2>
                <div class="sensor-row">Temperature: <span id="temp">-</span></div>
                <div class="sensor-row">Humidity: <span id="hum">-</span></div>
                <div class="sensor-row">Pressure: <span id="press">-</span></div>
                <div class="sensor-row">Motion: <span id="motion">-</span></div>
                <div class="sensor-row">Air Quality: <span id="air">-</span></div>
                <div class="sensor-row">Updated: <span id="ts">-</span></div>
	     </div>
        </div>

        <script>
            async function updateSensors() {
                try {
                    const res = await fetch('/sensor_data');
                    const data = await res.json();

                    document.getElementById('temp').textContent =
                        data.temperature !== null ? data.temperature + ' °C' : 'N/A';

                    document.getElementById('hum').textContent =
                        data.humidity !== null ? data.humidity + ' %' : 'N/A';

                    document.getElementById('air').textContent =
                        data.air_quality !== null ? data.air_quality : 'N/A';
                        
                    document.getElementById('press').textContent =
                        data.pressure !== null ? data.pressure + ' hPa' : 'N/A';

                    document.getElementById('motion').textContent =
                        data.motion === null ? 'N/A' : (data.motion ? 'YES' : 'NO');

                    document.getElementById('ts').textContent = data.timestamp;
                } catch (err) {
                    console.error('Sensor fetch error:', err);
                }
            }

            updateSensors();
            setInterval(updateSensors, 1000);
        </script>
    </body>
    </html>
    """


@app.route("/video_feed")
def video_feed():
    return Response(
        generate_frames(),
        mimetype="multipart/x-mixed-replace; boundary=frame"
    )


@app.route("/sensor_data")
def sensor_data():
    return jsonify(get_sensor_data())


if __name__ == "__main__":
    try:
        t = threading.Thread(target=sender_loop, daemon=True)
        t.start()
        app.run(host="0.0.0.0", port=5000, threaded=True)
    finally:
        try:
            picam2.stop()
        except Exception:
            pass
        try:
            bus.close()
        except Exception:
            pass
