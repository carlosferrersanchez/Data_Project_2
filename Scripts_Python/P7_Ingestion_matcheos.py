import time
from google.cloud import pubsub_v1
import json
from XX_Creacion_de_clases import BaseDeDatos

# Inicializar el cliente Pub/Sub y definir la ruta de la suscripción
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('edem-dp2', 'matches_suscription')

def callback(message):
    print(f"Recibido mensaje: {message}")
    # Decodificar el mensaje JSON
    data = json.loads(message.data.decode('utf-8'))
    print(f"Datos del mensaje: {data}")
    procesar_mensaje(data)
    message.ack()

def procesar_mensaje(data):
    # Verificar que el mensaje contenga todas las claves esperadas
    keys_esperadas = ['id_match', 'id_request', 'id_service_offer', 'id_route', 'current_position', 'seats_remaining']
    if not all(key in data for key in keys_esperadas):
        print("Mensaje no contiene todas las claves esperadas, ignorando.")
        return

    db = BaseDeDatos()

    # Extraer los datos necesarios del mensaje
    id_ride = data['id_match']
    id_request = data['id_request']
    id_service_offer = data['id_service_offer']
    id_route = data['id_route']
    x = data['current_position']['x']
    y = data['current_position']['y']
    seats_remaining = data['seats_remaining']

    # Consultas a la base de datos para obtener información adicional si es necesario
    id_driver = db.consultar("""
        SELECT id_driver
        FROM active_vehicles
        WHERE id_service_offer = %s
    """, [id_service_offer])[0][0]

    id_customer, real_drop_point, price, passengers = db.consultar("""
        SELECT id_customer, drop_point, price, passengers
        FROM ride_requests
        WHERE id_request = %s
    """, [id_request])[0]

    # Insertar el nuevo viaje en la base de datos
    db.ejecutar("""
        INSERT INTO rides (id_ride, id_service_offer, id_request, id_driver, id_route, id_customer, 
        real_pick_up_point, real_drop_point, price, passengers, ride_time_start, ride_time_end, ride_status, ride_current_position)
        VALUES (%s, %s, %s, %s, %s, %s, POINT(%s, %s), %s, %s, %s, NOW(), NOW(), 'Ongoing', POINT(%s, %s))
    """, (id_ride, id_service_offer, id_request, id_driver, id_route, id_customer, 
          x, y, real_drop_point, price, passengers, x, y))

    # Actualizar las tablas correspondientes con el nuevo estado
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

# Crear un futuro para manejar la suscripción de forma asíncrona
future = subscriber.subscribe(subscription_path, callback=callback)
print("Esperando mensajes... Presiona Ctrl+C para detener.")

try:
    # Bloque principal para mantener el script corriendo
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    # Manejar la interrupción para cerrar de forma limpia
    print("Deteniendo la suscripción...")
    future.cancel()

# Puedes agregar aquí cualquier limpieza adicional necesaria
