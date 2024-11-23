from kafka import KafkaConsumer
from configs import kafka_config
import json

alert_topics = ['AS_temperature_alerts', 'AS_humidity_alerts']

consumer = KafkaConsumer(
    *alert_topics,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  
    enable_auto_commit=True
)

print(f"Subscribed to topics: {alert_topics}")
print("Waiting for alerts...")

try:
    for message in consumer:
        alert = message.value  
        topic = message.topic

        print(f"ALERT from topic '{topic}':")
        print(f"  Sensor ID: {alert['sensor_id']}")
        print(f"  Timestamp: {alert['timestamp']}")
        if 'temperature' in alert:
            print(f"  Temperature: {alert['temperature']}°C")
        if 'humidity' in alert:
            print(f"  Humidity: {alert['humidity']}%")
        print(f"  Message: {alert['message']}")
        print("-" * 50)

except KeyboardInterrupt:
    print("\nStopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    print("Kafka consumer closed.")
