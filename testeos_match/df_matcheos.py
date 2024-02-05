import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json
from math import radians, cos, sin, asin, sqrt
import random
import string

# Función para calcular la distancia entre dos puntos
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radio de la Tierra en kilómetros
    return c * r * 1000  # Resultado en metros

# Función para generar un ID de 4 caracteres alfanuméricos
def generate_short_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=4))

# Transformación personalizada para encontrar coincidencias y generar mensajes específicos
class FindMatchesAndGenerateMessages(beam.DoFn):
    def process(self, element):
        customers, drivers = element['customers'], element['drivers']
        assigned_customers = set()

        for customer in customers:
            if customer['id_customer'] in assigned_customers:
                continue

            for driver in drivers:
                if customer['id_route'] == driver['id_route'] and \
                   customer['destination_location'] == driver['destination_location'] and \
                   haversine(customer['pickup_location']['x'], customer['pickup_location']['y'], 
                             driver['pickup_location']['x'], driver['pickup_location']['y']) < 50:
                    
                    id_match = generate_short_id()
                    matched_message = {
                        "ID_MATCH": id_match,
                        "ID_CUSTOMER": customer['id_customer'],
                        "ID_DRIVER": driver['id_driver'],
                        "ID_ROUTE": customer['id_route'],
                        "PICKUP_LOC": customer['pickup_location'],
                        "DEST_LOC": customer['destination_location']
                    }
                    
                    assigned_customers.add(customer['id_customer'])
                    yield matched_message
                    break

def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        project='edem-dp2',
        region='us-west1',
        runner='DataflowRunner',
        temp_location='gs://bucket_match_edem-dp2/temp',
        staging_location='gs://bucket_match_edem-dp2/staging',
        job_name='dataflow-matches',
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/all_data_suscription')
         | 'Decode' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
         | 'Window into' >> beam.WindowInto(beam.window.FixedWindows(15))
         | 'Find Matches and Generate Messages' >> beam.ParDo(FindMatchesAndGenerateMessages())
         | 'Encode to JSON' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
         | 'Write to PubSub' >> beam.io.WriteToPubSub(topic='projects/edem-dp2/topics/matches')
        )

if __name__ == '__main__':
    run()
