#!/usr/bin/env python3
"""
Sensor Fusion CDAG — single-image task that dispatches by DSF_TASK_NAME.

Topology:
    sensor-temp ──┐
                   ├──→ fuser ──→ router ─┬──→ alerts   (targeted publish)
    sensor-humid ─┘                       └──→ archive  (targeted publish)

API methods demonstrated:
    sensor-temp, sensor-humid : publish()           — broadcast to subscribers
    fuser                     : subscribe_all()     — fan-in from multiple peers
    router                    : subscribe()         — iterator from single peer
                                publish(to=...)     — targeted delivery
    alerts                    : @task.on() + run()  — callback event loop
    archive                   : subscribe()         — iterator consumption
"""

import os
import time
import random
import math
from dsf_sdk import DSFTask

task = DSFTask()
role = os.environ.get("DSF_TASK_NAME", "unknown")


# ── sensor-temp: publishes temperature readings ─────────────────────────
def run_sensor_temp():
    print(f"[{task.name}] starting temperature sensor (publish broadcast)", flush=True)
    counter = 0
    for counter in range(1, 2**63):
        reading = {
            "sensor": task.name,
            "type": "temperature",
            "id": counter,
            "value": round(20.0 + 10.0 * math.sin(counter / 50.0) + random.gauss(0, 1.5), 2),
            "unit": "C",
            "ts": time.time(),
        }
        task.publish(reading)
        if counter % 50 == 0:
            print(f"[{task.name}] published {counter} readings (last: {reading['value']}{reading['unit']})", flush=True)
        time.sleep(0.2)


# ── sensor-humid: publishes humidity readings ───────────────────────────
def run_sensor_humid():
    print(f"[{task.name}] starting humidity sensor (publish broadcast)", flush=True)
    counter = 0
    for counter in range(1, 2**63):
        reading = {
            "sensor": task.name,
            "type": "humidity",
            "id": counter,
            "value": round(50.0 + 20.0 * math.cos(counter / 80.0) + random.gauss(0, 3.0), 2),
            "unit": "%",
            "ts": time.time(),
        }
        task.publish(reading)
        if counter % 50 == 0:
            print(f"[{task.name}] published {counter} readings (last: {reading['value']}{reading['unit']})", flush=True)
        time.sleep(0.2)


# ── fuser: subscribe_all() fan-in from both sensors ────────────────────
def run_fuser():
    print(f"[{task.name}] starting fuser (subscribe_all fan-in)", flush=True)
    latest = {}
    fused_count = 0

    for peer, reading in task.subscribe_all():
        latest[reading["type"]] = reading

        # Once we have both sensor types, emit a fused record.
        if "temperature" in latest and "humidity" in latest:
            fused_count += 1
            temp = latest["temperature"]["value"]
            humid = latest["humidity"]["value"]

            # Compute heat index (simplified formula).
            heat_index = round(temp + 0.33 * humid / 100 * temp, 2)
            anomaly = heat_index > 30 or temp > 32 or humid > 85

            fused = {
                "id": fused_count,
                "temperature": temp,
                "humidity": humid,
                "heat_index": heat_index,
                "anomaly": anomaly,
                "ts": time.time(),
            }
            task.publish(fused)

            if fused_count % 25 == 0:
                status = "ANOMALY" if anomaly else "normal"
                print(
                    f"[{task.name}] fused {fused_count} records | "
                    f"temp={temp}C humid={humid}% HI={heat_index} [{status}]",
                    flush=True,
                )


# ── router: subscribe iterator + targeted publish ──────────────────────
def run_router():
    print(f"[{task.name}] starting router (subscribe + targeted publish)", flush=True)
    routed = 0
    alerts_sent = 0

    for fused in task.subscribe("fuser"):
        routed += 1

        # Always send to archive.
        task.publish(fused, to="archive")

        # Anomalies also go to alerts.
        if fused.get("anomaly"):
            alerts_sent += 1
            task.publish(fused, to="alerts")

        if routed % 25 == 0:
            print(
                f"[{task.name}] routed {routed} records ({alerts_sent} alerts)",
                flush=True,
            )


# ── alerts: callback style (@task.on + task.run) ───────────────────────
def run_alerts():
    print(f"[{task.name}] starting alert handler (@task.on callback)", flush=True)
    alert_count = [0]  # mutable for closure

    @task.on("router")
    def handle_alert(fused):
        alert_count[0] += 1
        print(
            f"[{task.name}] ALERT #{alert_count[0]}: "
            f"temp={fused['temperature']}C humid={fused['humidity']}% "
            f"HI={fused['heat_index']}",
            flush=True,
        )

    task.run()


# ── archive: subscribe iterator, logs everything ───────────────────────
def run_archive():
    print(f"[{task.name}] starting archive (subscribe iterator)", flush=True)
    archived = 0

    for record in task.subscribe("router"):
        archived += 1
        if archived % 25 == 0:
            status = "ANOMALY" if record.get("anomaly") else "normal"
            print(
                f"[{task.name}] archived {archived} records | "
                f"last: HI={record['heat_index']} [{status}]",
                flush=True,
            )


# ── dispatch ────────────────────────────────────────────────────────────
ROLES = {
    "sensor-temp": run_sensor_temp,
    "sensor-humid": run_sensor_humid,
    "fuser": run_fuser,
    "router": run_router,
    "alerts": run_alerts,
    "archive": run_archive,
}

if role not in ROLES:
    print(f"[{role}] unknown role, expected one of: {list(ROLES.keys())}", flush=True)
    exit(1)

ROLES[role]()
