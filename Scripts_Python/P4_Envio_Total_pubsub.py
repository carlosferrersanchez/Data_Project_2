import json
import time
from google.cloud import pubsub_v1
from XX_Creacion_de_clases import BaseDeDatos  # Asumiendo que tienes un módulo para manejar la conexión a la base de datos

def formatear_coordenadas(coordenadas):
    # Suponiendo que las coordenadas vienen en un formato 'lat,lon'
    coordenadas = coordenadas.replace('(', '').replace(')', '')
    lat, lon = map(float, coordenadas.split(','))
    return {"x": lon, "y": lat}

def enviar_datos_combinados(project_id, topic_id):
    while True:
        db = BaseDeDatos()
        # Consultas SQL
        consulta_vehiculos = """
            SELECT id_service_offer, current_position, seats_available, id_route
            FROM active_vehicles
            WHERE seats_available != 0 AND service_status != 'Ended'
        """
        consulta_clientes = """
            SELECT id_request, pick_up_point, passengers, id_route
            FROM ride_requests
            WHERE request_status = 'Waiting'
        """
        
        # Obtiene registros
        vehiculos_activos = db.consultar(consulta_vehiculos)
        clientes_activos = db.consultar(consulta_clientes)

        # Formatea registros
        vehiculos = [{
            "id_service_offer": v[0],
            "current_position": formatear_coordenadas(v[1]),
            "seats_available": v[2],
            "id_route": v[3]
        } for v in vehiculos_activos]

        clientes = [{
            "id_request": c[0],
            "pick_up_point": formatear_coordenadas(c[1]),
            "passengers": c[2],
            "id_route": c[3]
        } for c in clientes_activos]

        # Prepara el mensaje combinado
        mensaje = json.dumps({
            "vehiculos": vehiculos,
            "clientes": clientes
        })

        # Cliente de Pub/Sub para publicar mensajes
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        # Codifica y envía el mensaje
        data = mensaje.encode("utf-8")
        try:
            publish_future = publisher.publish(topic_path, data)
            publish_future.result()  # Espera a que la publicación se complete
            print("Mensaje combinado enviado")
            print(mensaje)
        except Exception as e:
            print(f"Ocurrió un error al enviar el mensaje: {e}")

        db.cerrar()
        time.sleep(10)  # Espera antes de enviar el próximo lote

# Configuración del proyecto y topic
project_id = "edem-dp2"
topic_id = "all_data"  # Asume que este es el topic al que quieres enviar ambos tipos de datos

enviar_datos_combinados(project_id, topic_id)