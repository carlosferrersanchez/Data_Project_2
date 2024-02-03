import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
import json
import argparse
import logging
import json
import logging

""" Helpful functions """

def parse_pubsub_message(message):
    """ Parses Pub/Sub message and extracts 'id_driver' and 'id_route'. """
    
    test = json.loads (message.decode('utf-8'))

    logging.info(test)
    return test
#Dofn de transformación
def run(argv=None):
    """ Main entry point; defines and runs the dataflow pipeline. """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id',
        required=True,
        help='GCP cloud project name.'
    )
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='PubSub subscription to read from.'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery table to write to.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        project=known_args.project_id
    )
    
    # Define el esquema de la tabla en formato JSON
    input_schema = {
        {"mode": "NULLABLE", "name": "id_driver", "type": "INTEGER"},
        {"mode": "NULLABLE", "name": "id_route", "type": "INTEGER"},
        {"mode": "NULLABLE", "name": "pickup_location", "type":"STRING"},
        {"mode": "NULLABLE", "name": "destination_location", "type":"STRING"}
    }
    
    
    # Parsea el esquema usando bigquery_tools.parse_table_schema_from_json
    import os  # Importa el módulo os
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'edem-dp2'  

    with beam.Pipeline(options=pipeline_options) as p:  # Corrige la sangría y asegúrate de que pipeline_options esté definido
        schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))  # Define la variable schema antes de su uso
        (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(subscription=known_args.input_subscription)
            | 'ParsePubSubMessage' >> beam.Map(parse_pubsub_message)
            | 'WriteToBigQuery' >> WriteToBigQuery(
                known_args.output_table,
                schema=schema,  # Utiliza el esquema definido anteriormente
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Starting pipeline...")
    run()

