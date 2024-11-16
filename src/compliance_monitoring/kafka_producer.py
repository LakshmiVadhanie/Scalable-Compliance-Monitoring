from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random


def create_transaction():
    """Generate sample transaction data"""
    transaction = {
        'transaction_id': str(random.randint(1000000, 9999999)),
        'timestamp': datetime.now().isoformat(),
        'amount': round(random.uniform(100, 1000000), 2),
        'sender': f'Account-{random.randint(1000, 9999)}',
        'receiver': f'Account-{random.randint(1000, 9999)}',
        'transaction_type': random.choice(['WIRE', 'ACH', 'INTERNAL']),
        'risk_score': random.uniform(0, 1)
    }
    return transaction


def produce_transactions():
    """Produce sample transaction data to Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        transaction = create_transaction()
        producer.send('transactions', value=transaction)
        print(f"Produced transaction: {transaction['transaction_id']}")
        time.sleep(0.5)  # Simulate real-time transactions


if __name__ == "__main__":
    print("Starting Kafka Producer...")
    produce_transactions()