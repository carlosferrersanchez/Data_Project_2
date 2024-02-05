from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials
from XX_Creacion_de_clases import BaseDeDatos
import json

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('edem-dp2', 'matches_suscription')


def callback(message):
    print(f"Recibido mensaje: {message}")
    data = json.loads(message.data.decode('utf-8'))
    print(f"Datos del mensaje: {data}")
    procesar_mensaje(data)
    message.ack()

def procesar_mensaje(data):
    keys_esperadas = ['id_match', 'id_request', 'id_service_offer', 'current_position', 'new_seats']
    if not all(key in data for key in keys_esperadas):
        print("Mensaje no contiene todas las claves esperadas, ignorando.")
        return
    db = BaseDeDatos()
    id_route = data['id_route']
    id_match = data['id_match']
    id_request = data['id_request']
    id_service_offer = data['id_service_offer']
    # Extrae las coordenadas x e y del current_position
    x = data['current_position']['x']
    y = data['current_position']['y']
    seats_remaining = data['new_seats']

    # Consultas para obtener datos necesarios de otras tablas
    id_driver, id_route = db.consultar("""
        SELECT id_driver, id_route
        FROM active_vehicles
        WHERE id_service_offer = %s
    """, [id_service_offer])[0]

    id_customer, real_drop_point, price, passengers = db.consultar("""
        SELECT id_customer, drop_point, price, passengers
        FROM ride_requests
        WHERE id_request = %s
    """, [id_request])[0]

    # Inserta un nuevo registro en la tabla rides
    db.ejecutar("""
        INSERT INTO rides (id_ride, id_service_offer, id_request, id_driver, id_route, id_customer, 
        real_pick_up_point, real_drop_point, price, passengers, ride_time_start, ride_time_end, ride_status, ride_current_position)
        VALUES (%s, %s, %s, %s, %s, %s, POINT(%s, %s), %s, %s, %s, NOW(), NOW(), 'Ongoing', POINT(%s, %s))
    """, (id_match, id_service_offer, id_request, id_driver, id_route, id_customer, 
          x, y, real_drop_point, price, passengers, x, y))

    # Actualiza las tablas ride_requests y active_vehicles
    db.ejecutar("""
        UPDATE ride_requests
        SET request_status = 'Matched'
        WHERE id_request = %s
    """, [id_request])

    db.ejecutar("""
        UPDATE active_vehicles
        SET service_status = 'With Passengers', seats_available = %s
        WHERE id_service_offer = %s
    """, (seats_remaining, id_service_offer))

# Inicia la suscripción y espera por mensajes
print("Esperando mensajes...")
while True:
    subscriber.subscribe(subscription_path, callback=callback)