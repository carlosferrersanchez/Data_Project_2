from flask import Flask, jsonify
import psycopg2
from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
from getpass import getpass

# Autenticación inicial: gcloud auth application-default login
password = getpass("Introduce la contraseña: ")

def get_inactive_car_and_driver():
    conn = psycopg2.connect(
        dbname = "DP2",
        user = "postgres",
        password = password,
        host = "34.38.87.73",
        port = "5432"
    )
    cur = conn.cursor()

    # Selecciona un conductor inactivo
    cur.execute("""
        SELECT id_driver
        FROM drivers
        WHERE driver_status = 'Inactive'
        AND id_driver NOT IN (
            SELECT id_driver
            FROM active_vehicles
        )                
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_driver = cur.fetchone()

    # Selecciona una ruta al azar
    cur.execute("""
        SELECT id_route
        FROM routes
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_route = cur.fetchone()


    if id_route:
        # Obtiene el primer y último checkpoint de la ruta
        cur.execute("""
            WITH ordered_checkpoints AS (
                SELECT checkpoint_number, location
                FROM checkpoint_routes
                WHERE id_route = %s
                ORDER BY checkpoint_number
            )
            SELECT location
            FROM ordered_checkpoints
            WHERE checkpoint_number = (SELECT MIN(checkpoint_number) FROM ordered_checkpoints)
            UNION ALL
            SELECT location
            FROM ordered_checkpoints
            WHERE checkpoint_number = (SELECT MAX(checkpoint_number) FROM ordered_checkpoints)
        """, (id_route[0],))

        first_location, last_location = cur.fetchall()

        def format_location(location):
            # Elimina paréntesis y divide la cadena en latitud y longitud
            lat, lon = location.replace('(', '').replace(')', '').split(',')
            return {"x": float(lat), "y": float(lon)}

        pickup_location = format_location(first_location[0]) if first_location else None
        destination_location = format_location(last_location[0]) if last_location else None

        route_points = (pickup_location, destination_location)
    else:
        route_points = (None, None)

    cur.close()
    conn.close()

    return id_driver, id_route, route_points


def publish_message(project_id, topic_id, message):
    """Publica un mensaje a un tópico de Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = message.encode("utf-8")

    try:
        publish_future = publisher.publish(topic_path, data)
        publish_future.result() 
        print(f"New vehicle activated: {message}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def send_new_active_vehicle():
    while True:
        id_driver, id_route, route_points = get_inactive_car_and_driver()

        # Prepara el mensaje incluyendo ubicaciones de inicio y final
        message = {
            'id_driver': id_driver[0] if id_driver else None,
            'id_route': id_route[0] if id_route else None,
            'pickup_location': route_points[0],
            'destination_location': route_points[1]
        }
        message_json = json.dumps(message)

        publish_message('edem-dp2', 'active_vehicles', message_json)

        time.sleep(5)

send_new_active_vehicle()
