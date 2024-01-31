from flask import Flask, jsonify
import psycopg2
from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
from getpass import getpass

#PRIMERO INTRODUCIR ESTO EN TERMINAL PARA AUTENTICARTE: gcloud auth application-default login
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

    cur.execute("""
        SELECT id_route
        FROM routes
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_route = cur.fetchone()

    cur.close()
    conn.close()

    return id_driver, id_route

def publish_message(project_id, topic_id, message):
    """Publishes a message to a Pub/Sub topic."""
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
        id_driver, id_route = get_inactive_car_and_driver()

        message = {
            'id_driver': id_driver[0] if id_driver else None,
            'id_route': id_route[0] if id_route else None
        }
        message_json = json.dumps(message)

        publish_message('edem-dp2', 'active_vehicles', message_json)

        time.sleep(5)

send_new_active_vehicle()