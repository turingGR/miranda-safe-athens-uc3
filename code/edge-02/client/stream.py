from flask import Flask, Response, jsonify
from picamera2 import Picamera2
from gpiozero import MotionSensor
import cv2
import time
import smbus2
import bme280
import traceback
from grove.adc import ADC
import requests
import threading

app = Flask(__name__)

# =====================
# SETTINGS
# =====================

NODE_ID = "edge-02"
NODE_IP = "192.168.2.12"
SERVER_INGEST_URL = "http://192.168.2.8:5000/ingest"

PIR_GPIO = 5
BME280_ADDR = 0x76

FRAME_WIDTH = 640
FRAME_HEIGHT = 480

# =====================
# CAMERA
# =====================
picam2 = Picamera2()
camera_config = picam2.create_video_configuration(
    main={"size": (FRAME_WIDTH, FRAME_HEIGHT), "format": "RGB888"}
)
picam2.configure(camera_config)
picam2.start()
time.sleep(2)

# =====================
# PIR
# =====================
pir = MotionSensor(PIR_GPIO)

# =====================
# BME280
# =====================
bus = smbus2.SMBus(1)
calibration_params = bme280.load_calibration_params(bus, BME280_ADDR)

# =====================
# GROVE ADC (ADS1115)
# =====================

adc = ADC()
GAS_PORT = 0
AIR_PORT = 2

# =====================
# SENSOR READ FUNCTIONS
# =====================

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

    except:
        return {"motion": None}


def read_gas():
    try:
        return {"gas": adc.read(GAS_PORT)}
    except Exception as e:
        print("Gas read error:", e)
        return {"gas": None}


def read_air():
    try:
        return {"air_quality": adc.read(AIR_PORT)}
    except Exception as e:
        print("Air quality read error:", e)
        return {"air_quality": None}


def get_sensor_data():

    data = {}

    data.update(read_bme280())
    data.update(read_pir())
    data.update(read_gas())
    data.update(read_air())

    data["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")

    return data


# =====================
# VIDEO STREAM
# =====================

def generate_frames():

    while True:

        try:

            frame = picam2.capture_array()
            frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

            s = get_sensor_data()

            cv2.putText(frame, f"T: {s['temperature']} C",
                        (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,255,0),2)

            cv2.putText(frame, f"H: {s['humidity']} %",
                        (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,255,0),2)

            cv2.putText(frame, f"P: {s['pressure']} hPa",
                        (10, 90), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,255,0),2)

            cv2.putText(frame, f"Motion: {s['motion']}",
                        (10, 120), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,255,255),2)

            ret, buffer = cv2.imencode(".jpg", frame)

            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" +
                buffer.tobytes() +
                b"\r\n"
            )

            time.sleep(0.03)

        except Exception as e:

            print("Frame error:", e)
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


# =====================
# ROUTES
# =====================

@app.route("/")
def index():

    return """
    <h1>IoT Camera Node</h1>

    <img src="/video_feed" width="640">

    <h2>Sensors</h2>

    <div id="data"></div>

<script>

async function update(){

let r = await fetch('/sensor_data')

let d = await r.json()

document.getElementById("data").innerHTML = `
Temp: ${d.temperature} C <br>
Humidity: ${d.humidity} % <br>
Pressure: ${d.pressure} hPa <br>
Motion: ${d.motion} <br>
Gas: ${d.gas} <br>
Air Quality: ${d.air_quality}
`

}

setInterval(update,1000)

update()

</script>
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


# =====================
# MAIN
# =====================

if __name__ == "__main__":
    t = threading.Thread(target=sender_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000, threaded=True)
