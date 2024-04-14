from confluent_kafka import Producer
import time
import requests
import json

#producer configuration
conf = {
    'bootstrap.servers': '10.0.2.15:9092',
    'client.id': 'console-producer'
}

# producer instance
producer = Producer(conf)

# Kafka topic
topic = 'aapl-price-data'

# Ticker symbol
ticker_symbol = 'aapl'

def fetch_and_send_stock_price():
    while True:
        try:
            url = 'https://query2.finance.yahoo.com/v8/finance/chart/aapl'
            response = requests.get(url)
            data = json.loads(response.text)
            price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            # Produce the stock price to the Kafka topic
            producer.produce(topic, key=ticker_symbol.encode('utf-8'), value=str(price).encode('utf-8'))
            producer.flush()
            print(f"Sent {ticker_symbol} price to Kafka: {price}")
        except Exception as e:
            print(f"Error fetching/sending stock price: {e}")

        # Sleep for a specified interval (e.g., 30 seconds) before fetching the next price
        time.sleep(30)

# Start sending stock price data
fetch_and_send_stock_price()
