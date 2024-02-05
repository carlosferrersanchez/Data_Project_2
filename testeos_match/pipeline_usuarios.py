from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import datetime
import json

class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            # Asume que todos los campos esperados están en el mensaje.
            output_record = {
                'id_user': record['id_customer'],  # Cambia el nombre de id_customer a id_user.
                'id_route': record['id_route'],
                'pickup_latitude': str(record['pickup_location']['x']),
                'pickup_longitude': str(record['pickup_location']['y']),
                'destination_latitude': str(record['destination_location']['x']),  # Asegúrate de ajustar el esquema de BigQuery para estos campos
                'destination_longitude': str(record['destination_location']['y']),
                'timestamp': datetime.utcnow().isoformat()
            }
            yield output_record
        except json.JSONDecodeError:
            print(f"JSONDecodeError para el elemento: {element}")
        except Exception as e:
            print(f"Error procesando el elemento: {element}, Error: {str(e)}")

def run():
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='edem-dp2',
        temp_location='gs://bucket_match_edem-dp2/temp',
        region='us-west1',
        streaming=True
    )

    schema = 'id_user:INTEGER, id_route:INTEGER, ' \
             'pickup_latitude:STRING, pickup_longitude:STRING, ' \
             'destination_latitude:STRING, destination_longitude:STRING, ' \
             'timestamp:TIMESTAMP'

    with beam.Pipeline(options=pipeline_options) as p:
        (p 
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/nuevo_tema_user-sub')
         | 'UTF-8 bytes to string' >> beam.Map(lambda x: x.decode('utf-8'))
         | 'Parse JSON Messages' >> beam.ParDo(ParseMessage())
         | 'WriteToBigQuery' >> WriteToBigQuery(
             'edem-dp2:bbdd_matcheos.sin_match_usuario',
             schema=schema,
             create_disposition=BigQueryDisposition.CREATE_NEVER,
             write_disposition=BigQueryDisposition.WRITE_APPEND,
             method="STREAMING_INSERTS"
         )
        )

if __name__ == '__main__':
    run()
