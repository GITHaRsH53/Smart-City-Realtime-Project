import json
import os
import random
from confluent_kafka import SerializingProducer   # we need to produce record
import simplejson
import uuid
from datetime import datetime, timedelta
import time

#IOT SERVICE PRODUCERS
LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100    # /100 breaks journey into 100 steps for simulating gradual movement.
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment Variables for configuration (required to configure Kafka properly and define the topics where different types of data will be published.)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')  # producer needs to know where the Kafka cluster is running
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')  # variables ensure the producer sends messages to the correct topics.
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()      # start time
start_location = LONDON_COORDINATES.copy()    # start location

#GENERATORS
def get_next_time():
    """
    Returns the current time in the format YYYY-MM-DDTHH:MM:SS -> ISO 8601 format
    """
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
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
        'humidity': random.randint(0, 100),  # percentage
        'airQualityIndex': random.uniform(0, 500)  # AQL Value goes here
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


def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):  # private car
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),  # km/h
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
    """
    Simulates the movement of a vehicle towards Birmingham by
    updating the latitude and longitude coordinates based on pre-calculated increments.
    Adds randomness to simulate road travel.
    """
    global start_location

    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(vehicle_id):
    """
    Generates vehicle data based on the provided
    vehicle_id and updates the vehicle_id by simulating vehicle movement.
    """
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),       #uuid (Universally Unique Identifier) is a 128-bit unique identifier used to uniquely label objects.
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(), # track of how long will it take it to reach Birmingham
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'Model S',
        'year': 2024,
        'fuelType': 'Electric'
    }

# PRODUCING IOT TO KAFKA
def json_serializer(obj):     # JSON serialization to convert data into byte format for Kafka.
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):      #  delivered to kafka
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),    # Assigns a unique key (UUID) to each message, ensuring better partitioning in Kafka.
        value=json.dumps(data, default=json_serializer).encode('utf-8'),   # Encodes it into bytes and put default as id may be not passed correctly
        on_delivery=delivery_report #
    )

    producer.flush()  # ensures that all messages are immediately sent to Kafka instead of waiting in the buffer.


def simulate_journey(producer, vehicle_id):  # all info about vechicle and produce data from here
    while True:                          # from central london to bermingham
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(vehicle_id, vehicle_data['timestamp'],
                                                           vehicle_data['location'], 'Intelligent AI-powered Camera')
        weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(vehicle_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])
        # Note: we added vehicle timestamp to all the objects as we want all data of that vehicle at same timestamp.

        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_incident_data)
        # break

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)


if __name__ == '__main__':
    producer_config = {                      # to configure kafka
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, # Specifies the Kafka broker(s)
        'error_cb': lambda err: print(f'Kafka Error: {err}')  # function that handles Kafka errors.
    }

    producer = SerializingProducer(producer_config)  # lib Used, as messages need to be serialized before publishing (python to json).

    try:
        simulate_journey(producer, 'Vehicle-Harsh-123') # Simulates vehicle movement and sends data to Kafka.
    except KeyboardInterrupt:       # Handles user stopping the simulation (CTRL+C).
        print('Simulation ended by the user.')
    except Exception as e:       # Kafka issues, JSON serialization failure, network issues)
        print(f'An unexpected error occurred: {e}')

