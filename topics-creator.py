from confluent_kafka.admin import AdminClient, NewTopic

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Topics to create
topics = [
    'topic1',
    'topic2',
    'topic3',
    # Add more topics as needed
]

#admin client
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

# Create new topics
new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
admin_client.create_topics(new_topics)

# Close admin client
admin_client.close()