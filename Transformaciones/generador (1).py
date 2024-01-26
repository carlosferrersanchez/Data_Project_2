import os
import json
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1
import time
import threading

def extraer_coordenadas(kml_file):
    tree = ET.parse(kml_file)
    root = tree.getroot()
    namespace = {'kml': 'http://www.opengis.net/kml/2.2'}
    coords = []
    for placemark in root.findall(".//kml:Placemark", namespace):
        for point in placemark.findall(".//kml:Point/kml:coordinates", namespace):
            # Asumiendo formato 'longitud,latitud,altitud'
            longitud, latitud, altitud = point.text.strip().split(',')
            # Reordenar y formatear como 'latitud,longitud,altitud'
            coord_formateada = f"{latitud},{longitud},{altitud}"
            coords.append(coord_formateada)
    return coords

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/sound-groove-411710/topics/edem_tema'

def publicar_coordenadas(coordenadas, id_usuario):
    while True:
        for coord in coordenadas:
            latitud, longitud, altitud = coord.split(',')
            
            coordenada_json = {
                'id': id_usuario,
                'latitud': float(latitud),
                'longitud': float(longitud),
                'nivel_del_mar': float(altitud)
            }

            mensaje_json = json.dumps(coordenada_json)
            publisher.publish(topic_path, data=mensaje_json.encode('utf-8'))
            print(f"Coordenadas JSON publicadas para {id_usuario}: {coordenada_json}")
            #time.sleep(1)

ruta_usuario_kml = os.path.join("rutas", "usuario.kml")
ruta_conductor_kml = os.path.join("rutas", "conductor.kml")
coordenadas_usuario = extraer_coordenadas(ruta_usuario_kml)
coordenadas_conductor = extraer_coordenadas(ruta_conductor_kml)

# Asignar IDs constantes
id_usuario = "usu-2"
id_conductor = "con-1"

threading.Thread(target=publicar_coordenadas, args=(coordenadas_usuario, id_usuario), daemon=True).start()
threading.Thread(target=publicar_coordenadas, args=(coordenadas_conductor, id_conductor), daemon=True).start()

while True:
    time.sleep(1)
