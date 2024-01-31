import psycopg2
from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
from getpass import getpass

#PRIMERO INTRODUCIR ESTO EN TERMINAL PARA AUTENTICARTE: gcloud auth application-default login
password = getpass("Introduce la contraseña: ")
def get_inactive_customer():
    conn = psycopg2.connect(
        dbname = "DP2",
        user = "postgres",
        password = password,
        host = "34.38.87.73",
        port = "5432"
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT id_customer
        FROM customers
        WHERE customer_status = 'Inactive'
        AND id_customer NOT IN (
            SELECT id_customer
            FROM ride_requests
        )                
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_customer = cur.fetchone()

    cur.execute("""
        SELECT id_route
        FROM routes
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_route = cur.fetchone()

    pickup_location = None
    if id_route:
        cur.execute("""
            SELECT location[0] as x, location[1] as y
            FROM checkpoint_routes
            WHERE id_route = %s
            ORDER BY RANDOM()
            LIMIT 1
        """, (id_route[0],))
        pickup_checkpoint = cur.fetchone()
        if pickup_checkpoint:
            pickup_location = [pickup_checkpoint[0],pickup_checkpoint[1]]

    destination_location = None
    if id_route:
        cur.execute("""
            SELECT location[0] as x, location[1] as y
            FROM checkpoint_routes
            WHERE id_route = %s
            ORDER BY checkpoint_number DESC
            LIMIT 1
        """, (id_route[0],))
        destination_checkpoint = cur.fetchone()
        if destination_checkpoint:
            destination_location = [destination_checkpoint[0],destination_checkpoint[1]]

    cur.close()
    conn.close()
    
    return id_customer, id_route, pickup_location, destination_location

def publish_message(project_id, topic_id, message):
    """Publishes a message to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = message.encode("utf-8")

    try:
        publish_future = publisher.publish(topic_path, data)
        publish_future.result()
        print(f"New customer activated: {message}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def send_new_active_customer():
    while True:
        id_customer, id_route, pickup_location, destination_location = get_inactive_customer()

        message = {
            'id_customer': id_customer[0] if id_customer else None,
            'id_route': id_route[0] if id_route else None,
            'pickup_location_X': pickup_location,
            'destination_location': destination_location
        }
        message_json = json.dumps(message)

        publish_message('edem-dp2', 'customer_request', message_json)

        time.sleep(5)

send_new_active_customer()