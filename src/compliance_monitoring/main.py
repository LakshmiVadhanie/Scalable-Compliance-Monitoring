import multiprocessing
from kafka_producer import produce_transactions
from spark_processor import process_stream


def main():
    """Main entry point for the compliance monitoring platform"""
    # Start Kafka producer in a separate process
    producer_process = multiprocessing.Process(target=produce_transactions)
    producer_process.start()

    # Start Spark streaming process
    try:
        process_stream()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        producer_process.terminate()


if __name__ == "__main__":
    print("Starting Compliance Monitoring Platform...")
    main()