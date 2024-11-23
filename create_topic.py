from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

my_name = "AS"

topic_names = ['AS_building_sensors' , 'AS_temperature_alerts', 'AS_humidity_alerts']
num_partitions = 3
replication_factor = 1

for topic in topic_names:
    new_topic = NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

[print(topic) for topic in admin_client.list_topics() if my_name in topic]

admin_client.close()
