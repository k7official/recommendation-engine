from kafka import KafkaProducer
import json

# Connect to Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def rate_product():
    user_id = input("Enter user ID: ")
    product_id = input("Enter product ID: ")
    rating = input("Enter rating: ")

    # Send the rating to the Kafka topic
    producer.send('product_ratings', {'user_id': user_id, 'product_id': product_id, 'rating': rating})

    print('Rating received and sent to processing')

    # Signal the end of data production by sending a special message
    end_of_production_message = {'user_id': None, 'product_id': None, 'rating': None}
    producer.send('product_ratings', value=end_of_production_message)


if __name__ == '__main__':
    print("Console-based Flask Kafka Producer")
    print("Press Ctrl+C to exit")

    try:
        while True:
            rate_product()
    except KeyboardInterrupt:
        print("\nExiting...")
