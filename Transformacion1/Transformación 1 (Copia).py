import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import datetime

# Descripción del script
"""
Script: Canalización de Transmisión de Dataflow

Descripción: Este script se encarga de procesar los mensajes recibidos por nuestra cola de mensajes desde dispositivos y realiza las siguientes tareas:

1. Analiza y transforma los mensajes de Pub/Sub en el formato deseado.
2. Escribe los datos transformados en BigQuery para su posterior análisis.


Pablo Ivorra
"""

# Define la función para obtener el esquema de la tabla en BigQuery
def get_table_schema():
    """
    Define el esquema de la tabla en BigQuery para almacenar los datos.
    """
    return {
        "fields": [
            {"name": "id_driver", "type": "STRING", "mode": "REQUIRED"},
            {"name": "id_route", "type": "STRING", "mode": "REQUIRED"},
            {"name": "status", "type": "STRING", "mode": "REQUIRED"},
            # Agrega otros campos según sea necesario
        ]
    }

# Parsea el mensaje de Pub/Sub
def parse_pubsub_message(message):
    """
    Parsea y transforma el mensaje de Pub/Sub en el formato deseado.
    """
    import json
    msg = json.loads(message.data)
    id_usuario = msg.get('id')
    latitud = msg.get('latitud')
    longitud = msg.get('longitud')
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    return {
        'id_driver': id_usuario,
        'id_route': msg.get('id_route'),
        'status': 'active',
        'latitud': latitud,
        'longitud': longitud,
        'timestamp': timestamp
    }

# Configura las opciones de la canalización
options = PipelineOptions(
    runner='DataflowRunner',
    project='edem-dp2',
    region='europe-west6',
    staging_location='gs://active_vehicles/staging',
    temp_location='gs://active_vehicles/temp',
    setup_file='/Users/paulglobal/Documents/GitHub/Data_Project_2/Transformacion1/setup.py'
)

# Crea la canalización
with beam.Pipeline(options=options) as p:
    (
        p
        | 'ReadFromPubSub' >> ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/active-vehicles-sub')
        | 'ParsePubSubMessage' >> beam.Map(parse_pubsub_message)
        | 'WriteToBigQuery' >> WriteToBigQuery(
            table='edem-dp2.Active_Vehicles',  # Utiliza el nombre real de tu tabla en BigQuery.
            schema=get_table_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        # Agrega 'LogInsertedData' >> beam.ParDo(LogInsertedDataFn()) si es necesario
    )
