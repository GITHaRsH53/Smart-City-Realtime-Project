import json
import os
import random
from confluent_kafka import SerializingProducer
import simplejson
import uuid
from config import configuration # This fetches the value from config.py
from datetime import datetime, timedelta
import time
import boto3  # For SNS

# IOT SERVICE PRODUCERS
LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# SNS CONFIG
SNS_TOPIC_ARN = configuration["SNS_TOPIC_ARN"]
# SNS_TOPIC_ARN = os.getenv("EMERGENCY_SNS_TOPIC_ARN")  # Should be set as env variable
sns_client = boto3.client('sns',region_name="us-east-1",aws_access_key_id=configuration["AWS_ACCESS_KEY"],
                          aws_secret_access_key=configuration["AWS_SECRET_KEY"])

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'camera_id': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'Model S',
        'year': 2024,
        'fuelType': 'Electric'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def publish_to_sns(data):
    try:
        message = json.dumps(data, default=json_serializer)
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Emergency Alert: " + data.get("type", "Unknown")
        )
        print("📢 SNS Alert Sent:", response['MessageId'])
    except Exception as e:
        print(f"❌ Failed to send SNS alert: {e}")

def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(vehicle_id, vehicle_data['timestamp'],
                                                           vehicle_data['location'], 'AI-Camera-1')
        weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(vehicle_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
                and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        # Only send alert for real emergencies
        if emergency_incident_data['type'] != 'None' and emergency_incident_data['status'] == 'Active':
            publish_to_sns(emergency_incident_data)

        time.sleep(5)


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Harsh-123')
    except KeyboardInterrupt:
        print('Simulation ended by the user.')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
