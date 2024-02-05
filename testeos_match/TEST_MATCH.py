import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
from apache_beam import window
import math

class FindMatches(beam.DoFn):
    def process(self, element):
        grouped_messages = element[1]
        
        if 'clientes' in grouped_messages and 'conductores' in grouped_messages:
            customers = grouped_messages['clientes']
            drivers = grouped_messages['conductores']

            for customer_msg in customers:
                customer_data = json.loads(customer_msg.decode('utf-8'))
                
                for driver_msg in drivers:
                    driver_data = json.loads(driver_msg.decode('utf-8'))
                    
                    if customer_data['id_route'] == driver_data['id_route']:
                        customer_pickup = customer_data['pickup_location']
                        driver_pickup = driver_data['pickup_location']
                        distance = self.haversine(
                            customer_pickup['y'], customer_pickup['x'],
                            driver_pickup['y'], driver_pickup['x']
                        )
                        
                        if distance <= 1000:
                            matched_data = {
                                'id_customer': customer_data['id_customer'],
                                'id_driver': driver_data['id_driver'],
                                'pickup_location': customer_pickup,
                                'destination_location': driver_pickup,
                            }
                            yield matched_data

    
    def haversine(self, lon1, lat1, lon2, lat2):
        # Convertir grados decimales a radianes
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
        
        # Fórmula de Haversine
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radio de la Tierra en kilómetros. Usar 3956 para millas.
        return c * r * 1000  # Retornar resultado en metros.

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'edem-dp2'
    google_cloud_options.job_name = 'find-matches-job-test12'
    google_cloud_options.staging_location = 'gs://bucket_match_edem-dp2/staging'
    google_cloud_options.temp_location = 'gs://bucket_match_edem-dp2/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        customer_msgs = (
            p 
            | "Read Customer Messages" >> ReadFromPubSub(subscription="projects/edem-dp2/subscriptions/subs_active_customers").with_output_types(bytes)
            | "Decode and Parse Customer JSON" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | 'Window into Fixed Intervals (customers)' >> beam.WindowInto(window.FixedWindows(30))
            | 'Key Customers' >> beam.Map(lambda msg: ('clientes', msg))
        )

        driver_msgs = (
            p 
            | "Read Driver Messages" >> ReadFromPubSub(subscription="projects/edem-dp2/subscriptions/subs_active_vehicles").with_output_types(bytes)
            | "Decode and Parse Driver JSON" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | 'Window into Fixed Intervals (vehicles)' >> beam.WindowInto(window.FixedWindows(30))
            | 'Key Drivers' >> beam.Map(lambda msg: ('conductores', msg))
        )

        joined_messages = (
            (customer_msgs, driver_msgs)
            | 'Join Messages' >> beam.CoGroupByKey()
        )

        matched_records = (
            joined_messages 
            | 'Find Matches' >> beam.ParDo(FindMatches())
        )

        def format_as_json_string(element):
            json_str = json.dumps(element)
            return json_str.encode('utf-8')

        matched_records | "Format as JSON String" >> beam.Map(format_as_json_string) | "Write to PubSub" >> WriteToPubSub(topic="projects/edem-dp2/topics/matches")

if __name__ == "__main__":
    run()
