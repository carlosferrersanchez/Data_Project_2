import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info("New PubSub Message: %s", output)

    return json.loads(output)

def run():
    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as p:
        (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/subs_active_vehicles')
            | "print" >> beam.Map(print)
        )

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")
    
    # Run Process
    run()