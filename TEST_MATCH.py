import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam import window
from geopy.distance import geodesic
import psycopg2
import logging
from getpass import getpass

# Definición de la clase FindFirstClosestPointFn
class FindFirstClosestPointFn(beam.DoFn):
    def __init__(self, max_distance=50):
        self.max_distance = max_distance

    def process(self, customer_request, active_vehicles):
        customer_location = tuple(customer_request['pickup_location'])
        for vehicle in active_vehicles:
            vehicle_location = tuple(vehicle['location'])
            distance = geodesic(customer_location, vehicle_location).meters
            if distance <= self.max_distance:
                yield customer_request, vehicle, distance
                return

# Funciones para formatear los datos para las tablas SQL
def write_conductor_sin_match(element):
    return (
        element['id_driver'],
        element['id_route'],
        'POINT({} {})'.format(*element['location']),
        'POINT({} {})'.format(*element['destination_location'])
    )

def write_usuario_sin_match(element):
    return (
        element['id_customer'],
        element['id_route'],
        'POINT({} {})'.format(*element['location']),
        'POINT({} {})'.format(*element['destination_location'])
    )

def write_matcheos(element):
    return (
        element[1]['id_driver'],
        element[0]['id_customer'],
        'POINT({} {})'.format(*element[1]['location']),
        'POINT({} {})'.format(*element[0]['destination_location'])
    )

class WriteToPostgres(beam.DoFn):
    def __init__(self, table_name, insert_statement):
        self.table_name = table_name
        self.insert_statement = insert_statement

    def start_bundle(self):
        self.conn = psycopg2.connect(
            dbname='DP2',
            user='postgres',
            password=password,
            host='34.38.87.73'
        )
        self.cursor = self.conn.cursor()

    def process(self, element):
        try:
            self.cursor.execute(self.insert_statement, element)
            self.conn.commit()
        except Exception as e:
            logging.error("Error writing to Postgres: %s", str(e))
            # Handle the exception appropriately

    def finish_bundle(self):
        self.cursor.close()
        self.conn.close()

# Pedir contraseña de PostgreSQL
password = getpass("Introduce la contraseña: ")

# Enable streaming mode in PipelineOptions
options = PipelineOptions(
    runner='DataflowRunner',
    project='edem-dp2',
    temp_location='gs://bucket_match_edem-dp2/temp',
    staging_location='gs://bucket_match_edem-dp2/staging',
    streaming=True
)
p = beam.Pipeline(options=options)

# Define the window duration
window_duration = 1  # 1 minutes

# Lectura y windowing de los mensajes de Pub/Sub
active_vehicles = (p
                   | 'Read Active Vehicles' >> ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/subs_active_vehicles')
                   | 'Parse Active Vehicles' >> beam.Map(json.loads)
                   | 'Window for Active Vehicles' >> beam.WindowInto(window.FixedWindows(window_duration * 60)))

customer_requests = (p
                     | 'Read Customer Requests' >> ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/subs_active_customers')
                     | 'Parse Customer Requests' >> beam.Map(json.loads)
                     | 'Window for Customer Requests' >> beam.WindowInto(window.FixedWindows(window_duration * 60)))

# Procesamiento de los datos
processed_data = ({'customer_requests': customer_requests, 'active_vehicles': active_vehicles}
                  | 'Group by key' >> beam.CoGroupByKey()
                  | 'Find Closest Points' >> beam.ParDo(FindFirstClosestPointFn()))

# Writing to Postgres - Conductores Sin Match
active_vehicles | 'Write Conductores Sin Match' >> beam.Map(write_conductor_sin_match) | 'To Postgres Conductores Sin Match' >> beam.ParDo(
    WriteToPostgres(
        table_name='conductores_sin_match',
        insert_statement='INSERT INTO conductores_sin_match (id_driver, id_route, location, destination_location) VALUES (%s, %s, %s, %s)'
    )
)

# Writing to Postgres - Usuarios Sin Match
customer_requests | 'Write Usuarios Sin Match' >> beam.Map(write_usuario_sin_match) | 'To Postgres Usuarios Sin Match' >> beam.ParDo(
    WriteToPostgres(
        table_name='usuarios_sin_match',
        insert_statement='INSERT INTO usuarios_sin_match (id_customer, id_route, location, destination_location) VALUES (%s, %s, %s, %s)'
    )
)

# Writing to Postgres - Matcheos
processed_data | 'Write Matcheos' >> beam.Map(write_matcheos) | 'To Postgres Matcheos' >> beam.ParDo(
    WriteToPostgres(
        table_name='matcheos',
        insert_statement='INSERT INTO matcheos (id_driver, id_customer, match_location, destination_location) VALUES (%s, %s, %s, %s)'
    )
)

# Ejecución del pipeline
p.run().wait_until_finish()
