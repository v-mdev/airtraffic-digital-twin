#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Producer único con detección + predicción online (River) integrado.
Publica en Kafka topic 'iot' (compatible con la configuración Telegraf que proporcionaste)
y publica alertas en 'iot_alerts' cuando se predice de forma encadenada >= 5 minutos.

Ajustes en CONFIG al inicio.
"""
import time
import json
import math
import signal
import sys
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Dict, List, Any

import numpy as np

from src.airtraffic.get_token import get_token_redis

try:
    import requests
except Exception as e:
    print("Falta la librería 'requests'. Instálala con: pip install requests")
    raise

try:
    from confluent_kafka import Producer
except Exception as e:
    print("Falta la librería 'confluent-kafka'. Instálala con: pip install confluent-kafka")
    raise

try:
    from river import linear_model, optim, preprocessing
except Exception:
    print("Falta la librería 'river'. Instálala con: pip install river")
    raise

# -----------------------
# CONFIG
# -----------------------
@dataclass
class Config:
    # Kafka
    bootstrap_servers: str = "localhost:9998"
    topic: str = "iot"
    alert_topic: str = "iot_alerts"

    # OpenSky bounding box (ajusta si quieres)
    lamin: float = 36.5
    lamax: float = 37.1
    lomin: float = -4.8
    lomax: float = -4.3

    # Frecuencia (segundos) -- debe cuadrar con Telegraf (10s en tu caso)
    sleep_sec: int = 10

    # Detección: igualdad exacta por defecto
    exact_equality_only: bool = True
    tol_lat: float = 0.0
    tol_lon: float = 0.0
    tol_vel: float = 0.0
    tol_alt: float = 0.0

    # Ventanas / límites
    history_size: int = 40
    max_prediction_duration_sec: int = 300  # 5 minutos
    lr: float = 0.02  # learning rate para SGD en River

CONFIG = Config()

# -----------------------
# Helpers
# -----------------------
def is_nan(x) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return True

def almost_equal(a: Optional[float], b: Optional[float], tol: float) -> bool:
    if a is None or b is None:
        return False
    if CONFIG.exact_equality_only:
        return a == b
    return abs(a - b) <= tol

def to_safe_number(x):
    """
    Devuelve None si x es NaN o no numérico, o el float normal si es convertible.
    """
    try:
        if x is None:
            return None
        if isinstance(x, float):
            return None if math.isnan(x) else x
        if isinstance(x, (int,)):
            return float(x)
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None

def sanitize_for_json(obj):
    """
    Reemplaza float('nan') por None recursivamente para evitar JSON no estándar.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(x) for x in obj]
    try:
        if isinstance(obj, float) and math.isnan(obj):
            return None
    except Exception:
        pass
    return obj

# -----------------------
# State + modelos online (River)
# -----------------------
@dataclass
class State:
    last_point: Optional[Dict] = None
    first_pred_ts: Optional[int] = None
    consec_pred_secs: int = 0

    hist_time: Deque[float] = field(default_factory=lambda: deque(maxlen=CONFIG.history_size))
    hist_lat: Deque[float] = field(default_factory=lambda: deque(maxlen=CONFIG.history_size))
    hist_lon: Deque[float] = field(default_factory=lambda: deque(maxlen=CONFIG.history_size))
    hist_vel: Deque[float] = field(default_factory=lambda: deque(maxlen=CONFIG.history_size))
    hist_alt: Deque[float] = field(default_factory=lambda: deque(maxlen=CONFIG.history_size))

    # Para los 3 datos originales
    initial_data: List[Dict] = field(default_factory=list)
    trained_initial: bool = False

    model_lat: Any = field(default_factory=lambda: linear_model.LinearRegression())
    model_lon: Any = field(default_factory=lambda: linear_model.LinearRegression())
    model_vel: Any = field(default_factory=lambda: linear_model.LinearRegression())
    model_alt: Any = field(default_factory=lambda: linear_model.LinearRegression())

    def add_initial(self, msg: Dict):
        # Solo añadir si todos los campos originales existen y no son NaN
        if all(msg[k] is not None and not is_nan(msg[k]) for k in ["latitude", "longitude", "velocity", "baro_altitude"]):
            self.initial_data.append(msg)
            print(f"[INIT_DEBUG] Added original data {len(self.initial_data)}/3: {msg}")
        else:
            print(f"[INIT_DEBUG] Ignored incomplete message: {msg}")

    def train_initial(self):
        if len(self.initial_data) < 3 or self.trained_initial:
            return

        print("[INIT_DEBUG] Training models with 3 initial messages and interpolations...")
        # Extraer arrays
        times = [d["time"] for d in self.initial_data]
        lats = [d["latitude"] for d in self.initial_data]
        lons = [d["longitude"] for d in self.initial_data]
        vels = [d["velocity"] for d in self.initial_data]
        alts = [d["baro_altitude"] for d in self.initial_data]

        # Interpolación lineal hasta 100 puntos
        times_interp = np.linspace(times[0], times[-1], 100)
        lats_interp = np.interp(times_interp, times, lats)
        lons_interp = np.interp(times_interp, times, lons)
        vels_interp = np.interp(times_interp, times, vels)
        alts_interp = np.interp(times_interp, times, alts)

        for t, lat, lon, vel, alt in zip(times_interp, lats_interp, lons_interp, vels_interp, alts_interp):
            x = {}
            self.model_lat.learn_one(x, lat)
            self.model_lon.learn_one(x, lon)
            self.model_vel.learn_one(x, vel)
            self.model_alt.learn_one(x, alt)

        self.trained_initial = True
        print("[INIT_DEBUG] Initial training done with 100 points.")


    def update_models_with(self, t: float, lat, lon, vel, alt):
        x = {}
        # Aprender solo valores válidos, ignorar nulos
        try:
            if lat is not None and not is_nan(lat):
                print(f"[UPDATE_DEBUG] Aprendiendo latitud: t={t}, lat={lat}")
                self.model_lat.learn_one(x, float(lat))
            else:
                print(f"[UPDATE_DEBUG] Latitud no válida, no se aprende: t={t}, lat={lat}")
        except Exception as e:
            print(f"[UPDATE_DEBUG] Error aprendiendo latitud: {e}")
        try:
            if lon is not None and not is_nan(lon):
                print(f"[UPDATE_DEBUG] Aprendiendo longitud: t={t}, lon={lon}")
                self.model_lon.learn_one(x, float(lon))
            else:
                print(f"[UPDATE_DEBUG] Longitud no válida, no se aprende: t={t}, lon={lon}")
        except Exception as e:
            print(f"[UPDATE_DEBUG] Error aprendiendo longitud: {e}")
        try:
            if vel is not None and not is_nan(vel):
                print(f"[UPDATE_DEBUG] Aprendiendo velocidad: t={t}, vel={vel}")
                self.model_vel.learn_one(x, float(vel))
            else:
                print(f"[UPDATE_DEBUG] Velocidad no válida, no se aprende: t={t}, vel={vel}")
        except Exception as e:
            print(f"[UPDATE_DEBUG] Error aprendiendo velocidad: {e}")
        try:
            if alt is not None and not is_nan(alt):
                print(f"[UPDATE_DEBUG] Aprendiendo altitud: t={t}, alt={alt}")
                self.model_alt.learn_one(x, float(alt))
            else:
                print(f"[UPDATE_DEBUG] Altitud no válida, no se aprende: t={t}, alt={alt}")
        except Exception as e:
            print(f"[UPDATE_DEBUG] Error aprendiendo altitud: {e}")

    def predict(self, t: float) -> Dict[str, Optional[float]]:
        x = {"t": float(t)}
        out = {}
        try:
            v = self.model_lat.predict_one(x)
            out["latitude"] = None if is_nan(v) else float(v)
        except Exception:
            out["latitude"] = None
        try:
            v = self.model_lon.predict_one(x)
            out["longitude"] = None if is_nan(v) else float(v)
        except Exception:
            out["longitude"] = None
        try:
            v = self.model_vel.predict_one(x)
            out["velocity"] = None if is_nan(v) else float(v)
        except Exception:
            out["velocity"] = None
        try:
            v = self.model_alt.predict_one(x)
            out["baro_altitude"] = None if is_nan(v) else float(v)
        except Exception:
            out["baro_altitude"] = None
        return out

# -----------------------
# Kafka producer
# -----------------------
def build_producer():
    return Producer({"bootstrap.servers": CONFIG.bootstrap_servers})

def _delivery_callback(err, msg):
    if err is not None:
        print("Error entregando mensaje:", err)

# -----------------------
# Lógica de decisión + predicción (las 5 reglas)
# -----------------------
def decide_and_predict(st: State, msg_in: Dict):
    """
    msg_in: diccionario con al menos: time, latitude, longitude, velocity, baro_altitude
    Devuelve (msg_out, alert_opt).
    Añade debug detallado sobre datos recibidos y predichos.
    """
    now_ts = int(msg_in.get("time") or time.time())
    lat = to_safe_number(msg_in.get("latitude"))
    lon = to_safe_number(msg_in.get("longitude"))
    vel = to_safe_number(msg_in.get("velocity"))
    alt = to_safe_number(msg_in.get("baro_altitude"))

    print(f"[DECIDE_DEBUG] Datos recibidos: time={now_ts}, lat={lat}, lon={lon}, vel={vel}, alt={alt}")

    reason = None
    predicted_fields: List[str] = []

    lp = st.last_point

    # 1) Todos iguales -> predecir todo
    if lp and all([
        almost_equal(lat, lp.get("latitude"), CONFIG.tol_lat),
        almost_equal(lon, lp.get("longitude"), CONFIG.tol_lon),
        almost_equal(vel, lp.get("velocity"), CONFIG.tol_vel),
        almost_equal(alt, lp.get("baro_altitude"), CONFIG.tol_alt),
    ]):
        reason = "frozen_all"
        preds = st.predict(now_ts)
        print(f"[DECIDE_DEBUG] Todos iguales, prediciendo todo: {preds}")
        lat = preds["latitude"]
        lon = preds["longitude"]
        vel = preds["velocity"]
        alt = preds["baro_altitude"]
        predicted_fields = ["latitude", "longitude", "velocity", "baro_altitude"]

    # 2) Solo lat o lon iguales exactamente -> predecir solo esa
    elif lp and almost_equal(lat, lp.get("latitude"), CONFIG.tol_lat) and not almost_equal(lon, lp.get("longitude"), CONFIG.tol_lon):
        reason = "lat_equal_only"
        pred_lat = st.predict(now_ts)["latitude"]
        print(f"[DECIDE_DEBUG] Lat igual, prediciendo latitud: {pred_lat}")
        lat = pred_lat
        predicted_fields.append("latitude")
    elif lp and almost_equal(lon, lp.get("longitude"), CONFIG.tol_lon) and not almost_equal(lat, lp.get("latitude"), CONFIG.tol_lat):
        reason = "lon_equal_only"
        pred_lon = st.predict(now_ts)["longitude"]
        print(f"[DECIDE_DEBUG] Lon igual, prediciendo longitud: {pred_lon}")
        lon = pred_lon
        predicted_fields.append("longitude")

    # 3) Si hay NaN -> predecir solo las NaN
    if is_nan(lat) or is_nan(lon) or is_nan(vel) or is_nan(alt):
        reason = (reason + "+nan") if reason else "nan_present"
        preds = st.predict(now_ts)
        print(f"[DECIDE_DEBUG] Hay NaN, predicciones: {preds}")
        if is_nan(lat):
            print(f"[DECIDE_DEBUG] Prediciendo latitud NaN: {preds['latitude']}")
            lat = preds["latitude"]
            predicted_fields.append("latitude")
        if is_nan(lon):
            print(f"[DECIDE_DEBUG] Prediciendo longitud NaN: {preds['longitude']}")
            lon = preds["longitude"]
            predicted_fields.append("longitude")
        if is_nan(vel):
            print(f"[DECIDE_DEBUG] Prediciendo velocidad NaN: {preds['velocity']}")
            vel = preds["velocity"]
            predicted_fields.append("velocity")
        if is_nan(alt):
            print(f"[DECIDE_DEBUG] Prediciendo altitud NaN: {preds['baro_altitude']}")
            alt = preds["baro_altitude"]
            predicted_fields.append("baro_altitude")

    # Etiquetado y control tiempo de predicción encadenada
    if predicted_fields:
        source = "predicted"
        if st.first_pred_ts is None:
            st.first_pred_ts = now_ts
            st.consec_pred_secs = 0
        else:
            st.consec_pred_secs = now_ts - st.first_pred_ts
        print(f"[DECIDE_DEBUG] Campos predichos: {predicted_fields}, razón: {reason}, duración predicción: {st.consec_pred_secs}s")
    else:
        source = "original"
        st.first_pred_ts = None
        st.consec_pred_secs = 0
        print(f"[DECIDE_DEBUG] Todos los datos originales, no se predice nada.")

    out = {
        "time": now_ts,
        "icao24": msg_in.get("icao24"),
        "callsign": msg_in.get("callsign"),
        "origin_country": msg_in.get("origin_country"),
        "latitude": None if is_nan(lat) else lat,
        "longitude": None if is_nan(lon) else lon,
        "velocity": None if is_nan(vel) else vel,
        "baro_altitude": None if is_nan(alt) else alt,
        "source": source,
        "predicted_fields": sorted(set(predicted_fields)),
        "prediction_reason": reason,
        "prediction_duration_sec": st.consec_pred_secs if source == "predicted" else 0,
    }

    print(f"[DECIDE_DEBUG] Salida final: {out}")

    # Aprendemos de todos los valores (originales o predichos)
    learn_lat = out["latitude"]
    learn_lon = out["longitude"]
    learn_vel = out["velocity"]
    learn_alt = out["baro_altitude"]

    st.hist_time.append(now_ts)
    st.hist_lat.append(float(learn_lat) if not is_nan(learn_lat) and learn_lat is not None else float("nan"))
    st.hist_lon.append(float(learn_lon) if not is_nan(learn_lon) and learn_lon is not None else float("nan"))
    st.hist_vel.append(float(learn_vel) if not is_nan(learn_vel) and learn_vel is not None else float("nan"))
    st.hist_alt.append(float(learn_alt) if not is_nan(learn_alt) and learn_alt is not None else float("nan"))

    st.update_models_with(now_ts, learn_lat, learn_lon, learn_vel, learn_alt)

    # 4) Alerta si llevamos >= 5 min prediciendo
    alert = None
    if source == "predicted" and st.consec_pred_secs >= CONFIG.max_prediction_duration_sec:
        alert = {
            "time": now_ts,
            "level": "error",
            "code": "NO_DATA_5MIN",
            "message": "Los datos no llegan desde hace 5 minutos",
            "prediction_duration_sec": st.consec_pred_secs,
        }
        print(f"[DECIDE_DEBUG] ALERTA disparada: {alert}")

    # Guardar último punto tal como llegó (antes de predicción) para siguiente comparación
    st.last_point = {
        "time": msg_in.get("time"),
        "latitude": msg_in.get("latitude"),
        "longitude": msg_in.get("longitude"),
        "velocity": msg_in.get("velocity"),
        "baro_altitude": msg_in.get("baro_altitude"),
    }

    return out, alert

# -----------------------
# Loop principal: consulta OpenSky y publica
# -----------------------
def run():
    producer = build_producer()
    st = State()
    running = True

    def shutdown(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Producer con predictor online arrancado. Ctrl+C para parar.")

    tracked_icao24 = None
    last_msg = None
    missing_since = None
    candidate_planes: Dict[str, List[Dict]] = {}

    while running:
        try:
            print("[DEBUG] Starting main loop iteration")
            lamin, lamax, lomin, lomax = CONFIG.lamin, CONFIG.lamax, CONFIG.lomin, CONFIG.lomax
            print(f"[DEBUG] Bounding box: lamin={lamin}, lamax={lamax}, lomin={lomin}, lomax={lomax}")
            TOKEN = get_token_redis()
            print(f"[DEBUG] Retrieved token: {TOKEN[:8]}...")  # type: ignore # solo mostramos parte del token
            headers = {"Authorization": f"Bearer {TOKEN}"}
            url = f"https://opensky-network.org/api/states/all?lamin={lamin}&lomin={lomin}&lamax={lamax}&lomax={lomax}"
            print(f"[DEBUG] Requesting OpenSky URL: {url}")
            response = requests.get(url, timeout=10, headers=headers)
            print(f"[DEBUG] OpenSky response status: {response.status_code}")
            data = response.json()
            print(f"[DEBUG] OpenSky response data keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            now = int(time.time())
            msg = None

            if isinstance(data, dict) and data.get("states"):
                states = data["states"]
                print(f"[DEBUG] Number of planes in area: {len(states)}")

                if tracked_icao24 is None:
                    # --- Escaneo de candidatos ---
                    print("[DEBUG] No plane currently tracked, scanning candidates...")
                    for s in states:
                        if not s:
                            continue
                        icao24 = s[0]
                        lat = to_safe_number(s[6])
                        lon = to_safe_number(s[5])
                        alt = to_safe_number(s[7])
                        vel = to_safe_number(s[9])

                        msg_candidate = {
                            "icao24": icao24,
                            "callsign": (s[1] or "").strip() or None,
                            "origin_country": s[2],
                            "longitude": lon,
                            "latitude": lat,
                            "baro_altitude": alt,
                            "velocity": vel,
                            "time": int(data.get("time") or now)
                        }

                        # Añadir solo si tiene todos los campos originales válidos
                        if all(msg_candidate[k] is not None and not is_nan(msg_candidate[k])
                               for k in ["latitude", "longitude", "velocity", "baro_altitude"]):
                            candidate_planes.setdefault(icao24, []).append(msg_candidate)
                            st.add_initial(msg_candidate)
                            print(f"[DEBUG] Candidate {icao24} buffer size: {len(candidate_planes[icao24])}")

                        # Entrenar al tener 3 mensajes completos
                        if len(candidate_planes.get(icao24, [])) >= 3 and not st.trained_initial:
                            tracked_icao24 = icao24
                            st.train_initial()
                            last_msg = candidate_planes[icao24][-1]
                            print(f"[DEBUG] Tracking new plane definitively: {tracked_icao24}")
                            break

                    if not tracked_icao24:
                        print("[DEBUG] No plane ready yet (less than 3 original messages). Waiting...")
                        time.sleep(CONFIG.sleep_sec)
                        continue

                else:
                    # --- Procesamiento del avión ya rastreado ---
                    print(f"[DEBUG] Looking for tracked plane: {tracked_icao24}")
                    tracked_state = next((s for s in states if s and s[0] == tracked_icao24), None)
                    if tracked_state:
                        print(f"[DEBUG] Found tracked plane: {tracked_icao24}")
                        missing_since = None
                        msg = {
                            "icao24": tracked_state[0],
                            "callsign": (tracked_state[1] or "").strip() or None,
                            "origin_country": tracked_state[2],
                            "longitude": to_safe_number(tracked_state[5]),
                            "latitude": to_safe_number(tracked_state[6]),
                            "baro_altitude": to_safe_number(tracked_state[7]),
                            "velocity": to_safe_number(tracked_state[9]),
                            "time": int(data.get("time") or now)
                        }
                        print(f"[DEBUG] Message for decide_and_predict: {msg}")
                        last_msg = msg
                        processed, alert = decide_and_predict(st, msg)
                        print(f"[DEBUG] Processed message: {processed}")
                        if alert:
                            print(f"[DEBUG] Alert generated: {alert}")
                    else:
                        print(f"[DEBUG] Plane {tracked_icao24} disappeared from states")
                        if last_msg is None:
                            print("[DEBUG] Plane disappeared but no last_msg, waiting...")
                            time.sleep(CONFIG.sleep_sec)
                            continue

                        if missing_since is None:
                            missing_since = now
                            print(f"[DEBUG] Plane missing_since set to {missing_since}")
                        elapsed = now - missing_since
                        print(f"[DEBUG] Plane missing for {elapsed} seconds")

                        msg = dict(last_msg)
                        msg["time"] = now
                        print(f"[DEBUG] Using last known position for decide_and_predict: {msg}")
                        processed, alert = decide_and_predict(st, msg)
                        print(f"[DEBUG] Processed message (missing): {processed}")

                        if elapsed >= 60 and elapsed < 120:
                            alert = {
                                "time": now,
                                "level": "warning",
                                "code": "MISSING_1MIN",
                                "message": f"Plane {tracked_icao24} missing data for 1+ min",
                                "last_position": last_msg
                            }
                            print(f"[DEBUG] Warning alert: {alert}")
                        elif elapsed >= 120:
                            print(f"[DEBUG] Plane {tracked_icao24} considered dead, selecting new plane")
                            tracked_icao24 = None
                            last_msg = None
                            missing_since = None
                            candidate_planes.clear()
                            time.sleep(CONFIG.sleep_sec)
                            continue

                    # --- Publicación Kafka ---
                    processed = sanitize_for_json(processed)
                    print(f"[DEBUG] Publishing processed message to Kafka topic '{CONFIG.topic}': {processed}")
                    try:
                        producer.produce(CONFIG.topic, value=json.dumps(processed), callback=_delivery_callback)
                    except Exception as e:
                        print("[DEBUG] Error producing to Kafka:", e)

                    if alert:
                        print(f"[DEBUG] Publishing alert to Kafka topic '{CONFIG.alert_topic}': {alert}")
                        try:
                            producer.produce(CONFIG.alert_topic, value=json.dumps(alert), callback=_delivery_callback)
                        except Exception as e:
                            print("[DEBUG] Error producing alert to Kafka:", e)

                    producer.poll(0)
                    print(f"[DEBUG] Sleeping for {CONFIG.sleep_sec} seconds")
                    time.sleep(CONFIG.sleep_sec)

            else:
                print("[DEBUG] OpenSky returned no data or no planes in area")
                time.sleep(CONFIG.sleep_sec)

        except KeyboardInterrupt:
            print("[DEBUG] KeyboardInterrupt received, breaking loop")
            break
        except Exception as e:
            print("[DEBUG] Error in main loop:", e, file=sys.stderr)
            time.sleep(5)

    try:
        print("[DEBUG] Flushing producer")
        producer.flush(5)
    except Exception:
        print("[DEBUG] Error flushing producer")
        pass
    print("[DEBUG] Producer stopped.")



if __name__ == "__main__":
    run()