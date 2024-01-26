import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from haversine import haversine
import json
from apache_beam import window
import logging

class ProcesarCoordenadas(beam.DoFn):
    def process(self, elemento):
        try:
            elemento_json = json.loads(elemento.decode('utf-8'))
            if not isinstance(elemento_json, dict):
                logging.error("El elemento no es un diccionario válido: {}".format(elemento))
                return
            id_usuario = elemento_json.get('id')
            latitud = elemento_json.get('latitud')
            longitud = elemento_json.get('longitud')
            nivel_del_mar = elemento_json.get('nivel_del_mar')
            if None in [id_usuario, latitud, longitud, nivel_del_mar]:
                logging.error("Faltan datos en el elemento: {}".format(elemento))
                return
            yield {
                'id': id_usuario,
                'latitud': latitud,
                'longitud': longitud,
                'nivel_del_mar': nivel_del_mar
            }
        except Exception as e:
            logging.error("Error en la conversión: {}, elemento: {}".format(e, elemento))

def estan_cerca(elemento):
    conductor = list(elemento['conductor'])
    usuario = list(elemento['usuario'])
    
    if not conductor or not usuario:
        logging.info("No hay datos suficientes para comparar.")
        return

    for punto_conductor in conductor:
        for punto_usuario in usuario:
            coord_conductor = (punto_conductor['latitud'], punto_conductor['longitud'])
            coord_usuario = (punto_usuario['latitud'], punto_usuario['longitud'])
            if haversine(coord_conductor, coord_usuario, unit='m') <= 200:
                yield {
                    'conductor': punto_conductor,
                    'usuario': punto_usuario
                }

def format_to_bq(coincidencia):
    id_combinado = f"{coincidencia['conductor']['id']}_{coincidencia['usuario']['id']}"
    return {
        'id_combinado': id_combinado,
        'u_latitud': coincidencia['usuario']['latitud'],
        'u_longitud': coincidencia['usuario']['longitud'],
        'u_nivel_del_mar': coincidencia['usuario']['nivel_del_mar'],
        'c_latitud': coincidencia['conductor']['latitud'],
        'c_longitud': coincidencia['conductor']['longitud'],
        'c_nivel_del_mar': coincidencia['conductor']['nivel_del_mar']
    }

pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='sound-groove-411710',
    streaming=True,
    region='us-west1'
)

with beam.Pipeline(options=pipeline_options) as p:
    coordenadas = (p
                   | 'LeerDePubSub' >> beam.io.ReadFromPubSub(subscription='')
                   | 'ParsearCoordenadas' >> beam.ParDo(ProcesarCoordenadas())
                   | 'AplicarVentanas' >> beam.WindowInto(window.FixedWindows(30))
                  )

    conductor = coordenadas | 'FiltrarConductor' >> beam.Filter(lambda x: x['id'] == 'conductor')
    usuario = coordenadas | 'FiltrarUsuario' >> beam.Filter(lambda x: x['id'] == 'usuario')

    cercania = ({
        'conductor': conductor,
        'usuario': usuario
    }
                | 'Combinar' >> beam.CoGroupByKey()
                | 'ComprobarCercania' >> beam.FlatMap(estan_cerca)
               )

    coincidencias = cercania | 'FiltrarCoincidencias' >> beam.Filter(lambda x: isinstance(x, dict))
    coincidencias | 'FormatoParaBigQuery' >> beam.Map(format_to_bq) \
                  | 'EscribirEnBigQuery' >> beam.io.WriteToBigQuery(
                        'sound-groove-411710.edem_conjutodatos.coincidencias',
                        schema='id_combinado:STRING, u_latitud:FLOAT, u_longitud:FLOAT, u_nivel_del_mar:FLOAT, c_latitud:FLOAT, c_longitud:FLOAT, c_nivel_del_mar:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)



