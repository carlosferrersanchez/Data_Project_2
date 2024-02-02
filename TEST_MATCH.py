import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from geopy.distance import geodesic

class FindMatchFn(beam.DoFn):
    def process(self, element, users, drivers):
        user_data = users[element['id_route']]
        driver_data = drivers[element['id_route']]

        if user_data and driver_data:
            distance = geodesic((user_data['lat'], user_data['lon']), (driver_data['lat'], driver_data['lon'])).meters
            if distance <= 50:
                # Coincidencia encontrada
                yield {
                    'id_user': user_data['id'], 
                    'id_driver': driver_data['id'], 
                    'id_route': element['id_route'], 
                    'lat': str(user_data['lat']),  # Convertir a string
                    'lon': str(user_data['lon']),  # Convertir a string
                    'distance': str(distance)      # Convertir a string
                }

def run():
    # Configurar opciones para el pipeline
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'edem-dp2'
    google_cloud_options.region = 'us-west1'
    google_cloud_options.staging_location = 'gs://bucket_match_edem-dp2/staging'
    google_cloud_options.temp_location = 'gs://bucket_match_edem-dp2/temp'
    options.view_as(StandardOptions).streaming = True  # Habilitar modo streaming

    with beam.Pipeline(options=options) as p:
        # Leer mensajes de suscripciones de Pub/Sub para usuarios y conductores
        users = p | 'Leer Mensajes Usuarios' >> beam.io.ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/subs_active_customers')
        drivers = p | 'Leer Mensajes Conductores' >> beam.io.ReadFromPubSub(subscription='projects/edem-dp2/subscriptions/subs_active_vehicles')

        # Unir los PCollection de usuarios y conductores
        combined = ((users, drivers) | beam.Flatten())

        # Encontrar coincidencias
        matches = (combined | beam.ParDo(FindMatchFn(), beam.pvalue.AsDict(users), beam.pvalue.AsDict(drivers)))

        # Escribir coincidencias en BigQuery
        matches | 'Escribir Coincidencias en BigQuery' >> beam.io.WriteToBigQuery('edem-dp2.bbd_matcheos.matches_test')


if __name__ == '__main__':
    run()
