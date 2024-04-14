from confluent_kafka import Producer
import time
import requests
import json

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'client.id': 'economic-data-producer'
}

# Create a Kafka producer instance for economic data
economic_producer = Producer(conf)

# Dictionary to store the last update time for each data point
last_update_times = {
    'UNRATE': 0,  # Example: last update time for the unemployment rate
    'FEDFUNDS': 0  # Example: last update time for the federal funds rate
    # Add more data points as needed
}

def fetch_and_send_economic_data():
    while True:
        try:
            for series_id, last_update_time in last_update_times.items():
                # Example economic data request from the FRED API (replace with your actual API request)
                url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key=your_api_key'
                response = requests.get(url)
                data = json.loads(response.text)
                # Assume the API response contains the latest data point
                latest_data = data["observations"][0]
                if latest_data["date"] > last_update_time:
                    value = latest_data["value"]
                    topic = f'economic-data-{series_id.lower()}-topic'  # Topic for economic data
                    # Produce the economic data to the Kafka topic
                    economic_producer.produce(topic, value=str(value).encode('utf-8'))
                    economic_producer.flush()
                    print(f"Sent economic data ({series_id}) to Kafka: {value}")
                    # Update the last update time for the data point
                    last_update_times[series_id] = latest_data["date"]
        except Exception as e:
            print(f"Error fetching/sending economic data: {e}")

        # Sleep for a specified interval (e.g., 30 seconds) before checking for updates
        time.sleep(30)

# Start streaming economic data
fetch_and_send_economic_data()