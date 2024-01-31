""" Import libraries """
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import json
import argparse
import logging

""" Helper function to parse Pub/Sub messages """
def parse_pubsub_message(message):
    """ Parses Pub/Sub message. """
    data = json.loads(message.data.decode('utf-8'))
    id_driver = data.get('id:driver')
    id_route = data.get('id_route')
    return {"id": id_driver, "id_route": id_route}

""" Helper function to define BigQuery table schema """
def get_table_schema():
    """ Defines the BigQuery table schema. """
    table_schema_json = json.dumps({
        "fields": [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "latitud", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitud", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "altitud", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    })
    return parse_table_schema_from_json(table_schema_json)

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

    # Pipeline options
    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        project=known_args.project_id
    )

    # The pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(subscription=known_args.input_subscription)
            | 'ParsePubSubMessage' >> beam.Map(parse_pubsub_message)
            | 'WriteToBigQuery' >> WriteToBigQuery(
                known_args.output_table,
                schema=get_table_schema(),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Starting pipeline...")
    run()

