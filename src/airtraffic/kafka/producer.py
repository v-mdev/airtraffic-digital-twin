
from src.airtraffic.get_token import get_token_redis
from confluent_kafka import Producer
import requests
import time
import json

def delivery_callback(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message sent to topic %s partition %s with latency %f" %
              (str(msg.topic()), str(msg.partition()), msg.latency()))

def main():
    print("Starting Kafka Producer...")
    conf = {'bootstrap.servers': "localhost:9092"}
    producer = Producer(conf)

    TOPIC = "iot"

    print(f"Using OpenSky credentials: TOPIC={TOPIC}")

    lamin, lamax = 36.5, 37.5
    lomin, lomax = -5.0, -3.5
    print("Preparing to get token...")
    TOKEN = get_token_redis()
    print("Token:"+ f"{TOKEN}")
    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }
    url = f"https://opensky-network.org/api/states/all?lamin={lamin}&lomin={lomin}&lamax={lamax}&lomax={lomax}"
    print(f"Requesting data from OpenSky: {url}")
    response = requests.get(url, timeout=10, headers=headers)
    print(f"Response status code: {response.status_code}")
    data = response.json()
    print(f"Received data: {json.dumps(data)[:200]}...")  # Print first 200 chars

    tracked_icao24 = None  # ICAO24 del avión a seguir
    last_msg = None        # Último estado conocido del avión

    while True:
        try:
            if "states" in data and data["states"]:
                if tracked_icao24 is None:
                    tracked_icao24 = data["states"][0][0]  # ICAO24 del primer avión
                    print(f"Tracking ICAO24: {tracked_icao24}")

                # Busca el avión por ICAO24
                tracked_state = next((state for state in data["states"] if state[0] == tracked_icao24), None)
                if tracked_state:
                    msg = {
                        "icao24": tracked_state[0],
                        "callsign": tracked_state[1].strip() if tracked_state[1] else None,
                        "origin_country": tracked_state[2],
                        "longitude": tracked_state[5],
                        "latitude": tracked_state[6],
                        "baro_altitude": tracked_state[7],
                        "velocity": tracked_state[9],
                        "time": data["time"]
                    }
                    last_msg = msg  # Actualiza el último estado conocido
                    print(f"Producing message to Kafka: {msg}")
                    producer.produce(TOPIC, value=json.dumps(msg), callback=delivery_callback)
                else:
                    print(f"Tracked ICAO24 {tracked_icao24} not found in this batch.")
                    if last_msg:
                        print(f"Sending last known state for {tracked_icao24}: {last_msg}")
                        producer.produce(TOPIC, value=json.dumps(last_msg), callback=delivery_callback)
                    else:
                        print("No previous state available to send.")
            else:
                print("No 'states' key found in response or no aircraft data.")
                if last_msg:
                    print(f"Sending last known state for {tracked_icao24}: {last_msg}")
                    producer.produce(TOPIC, value=json.dumps(last_msg), callback=delivery_callback)
                else:
                    print("No previous state available to send.")

            producer.poll(0)
            producer.flush()
            print("Batch sent to Kafka. Sleeping for 5 seconds...\n")
            time.sleep(5)

        except Exception as e:
            print("Error:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()