import json
import time
from XX_Creacion_de_clases import BaseDeDatos
from google.cloud import pubsub_v1

def enviar_clientes_activos(project_id, topic_id):
    while True:
        db = BaseDeDatos()
        # Consulta para obtener vehículos activos con asientos disponibles y servicio no finalizado
        consulta_sql = """
            SELECT id_request, pick_up_point, passengers, id_route
            FROM ride_requests
            WHERE request_status = 'Waiting'
        """
        registros = db.consultar(consulta_sql)

        # Cliente de Pub/Sub para publicar mensajes
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        for registro in registros:
            id_request, pick_up_point, passengers, id_route = registro
            # Prepara el mensaje como un objeto JSON
            mensaje = json.dumps({
                "id_request": id_request,
                "pick_up_point": pick_up_point,
                "passengers": passengers,
                "id_route": id_route
            })
            # Codifica el mensaje para publicarlo
            data = mensaje.encode("utf-8")
            
            # Publica el mensaje
            try:
                publish_future = publisher.publish(topic_path, data)
                publish_future.result()  # Espera a que la publicación se complete
                print(f"Mensaje enviado para la solicitud {id_request}: {mensaje}")
            except Exception as e:
                print(f"Ocurrió un error al enviar el mensaje: {e}")

        db.cerrar()
        time.sleep(10)

enviar_clientes_activos("edem-dp2",'customer_request')