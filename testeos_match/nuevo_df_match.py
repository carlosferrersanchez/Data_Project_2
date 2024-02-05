import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json
from math import radians, cos, sin, asin, sqrt
import random
import string
import logging

def haversine(lon1, lat1, lon2, lat2):
    # Cálculo de la distancia Haversine entre dos puntos
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radio de la Tierra en kilómetros
    return c * r * 1000  # Resultado en metros

class FindMatchesAndGenerateMessages(beam.DoFn):
    def __init__(self):
        self.id_match_counter = 0  # Inicializa el contador a 0

    def process(self, element):
        # Asegúrate de que tanto 'clientes_activos' como 'vehiculos_activos' están presentes en el mensaje.
        if 'clientes' not in element or 'vehiculos' not in element:
            logging.error("El elemento no contiene las claves 'clientes' o 'vehiculos'.")
            yield  # Sale temprano si alguna clave falta.

        # Procesa las listas de clientes y vehículos activos.
        clientes_activos = element['clientes']
        vehiculos_activos = element['vehiculos']

        # Itera sobre cada cliente activo.
        for cliente_activo in clientes_activos:
            # Extrae la latitud y longitud del punto de recogida del cliente.
            cliente_lat, cliente_lon = cliente_activo['pick_up_point']['x'], cliente_activo['pick_up_point']['y']

            # Itera sobre cada vehículo activo.
            for vehiculo_activo in vehiculos_activos:
                # Extrae la latitud y longitud de la posición actual del vehículo.
                vehiculo_lat, vehiculo_lon = vehiculo_activo['current_position']['x'], vehiculo_activo['current_position']['y']

                # Comprueba si hay coincidencia basada en la ruta, la distancia y la disponibilidad de asientos.
                if cliente_activo['id_route'] == vehiculo_activo['id_route'] and \
                   haversine(cliente_lon, cliente_lat, vehiculo_lon, vehiculo_lat) < 50 and \
                   vehiculo_activo['seats_available'] >= cliente_activo['passengers']:
                    
                    # Incrementa el contador para obtener un nuevo valor de "id_match".
                    self.id_match_counter += 1
                    id_match = self.id_match_counter

                    # Calcula los asientos restantes después de la asignación.
                    seats_remaining = vehiculo_activo['seats_available'] - cliente_activo['passengers']
                    
                    # Prepara el mensaje de coincidencia para ser emitido.
                    matched_message = {
                        "id_match": id_match,
                        "id_request": cliente_activo['id_request'],
                        "id_service_offer": vehiculo_activo['id_service_offer'],
                        "id_route": cliente_activo['id_route'],
                        "current_position": vehiculo_activo['current_position'],
                        "seats_remaining": seats_remaining  # Incluye los asientos restantes en el mensaje.
                    }

                    # Emite el mensaje de coincidencia.
                    yield matched_message

def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        project='edem-dp2',
        region='us-west1',
        runner='DataflowRunner',
        temp_location='gs://bucket_match_edem-dp2/temp',
        staging_location='gs://bucket_match_edem-dp2/staging',
        job_name='dataflow-matches1',
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
