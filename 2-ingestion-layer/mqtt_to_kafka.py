import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- KONFIGURASI PENTING ---
MQTT_BROKER = "localhost"
MQTT_TOPIC = "health/data"   # <--- HARUS SAMA DENGAN SENSOR
KAFKA_TOPIC = "health_stream"
# ---------------------------

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_connect(client, userdata, flags, rc):
    print("Bridge MQTT-Kafka Aktif. Menunggu data...")
    # Subscribe ke topik yang benar
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        # 1. Terima dari MQTT
        payload = json.loads(msg.payload.decode())
        print(f"\n[MQTT] Terima dari '{msg.topic}': {payload}")
        
        # 2. Kirim ke Kafka
        producer.send(KAFKA_TOPIC, payload)
        producer.flush()
        print(f"[KAFKA] Teruskan ke '{KAFKA_TOPIC}' -> SUKSES")
        
    except Exception as e:
        print(f"[ERROR] {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, 1883, 60)
client.loop_forever()