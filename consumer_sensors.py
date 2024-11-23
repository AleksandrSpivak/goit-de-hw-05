from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    'AS_building_sensors', 
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

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

temperature_alerts_topic = 'AS_temperature_alerts'
humidity_alerts_topic = 'AS_humidity_alerts'

print("Subscribed to topic 'AS_building_sensors'...")

try:
    for message in consumer:

        data = message.value
        sensor_id = data.get('sensor_id')
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        timestamp = data.get('timestamp')

        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": "High temperature detected!"
            }
            producer.send(temperature_alerts_topic, key=str(sensor_id), value=alert)
            producer.flush()
            print(f"Temperature alert sent: {alert}")

        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": "Humidity out of range detected!"
            }
            producer.send(humidity_alerts_topic, key=str(sensor_id), value=alert)
            producer.flush()
            print(f"Humidity alert sent: {alert}")
except KeyboardInterrupt:
    print("\nStopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()
    print("Kafka consumer and producer closed.")


