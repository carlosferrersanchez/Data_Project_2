import psycopg2
from google.cloud import pubsub_v1
import json
import time
from getpass import getpass

# Autenticación inicial: gcloud auth application-default login
password = getpass("Introduce la contraseña: ")

def get_inactive_customers(n=5):
    conn = psycopg2.connect(
        dbname="DP2",
        user="postgres",
        password=password,
        host="34.38.87.73",
        port="5432"
    )
    cur = conn.cursor()
    customers_data = []

    # Obtener n clientes inactivos
    cur.execute("""
        SELECT id_customer
        FROM customers
        WHERE customer_status = 'Inactive'
        AND id_customer NOT IN (
            SELECT id_customer
            FROM ride_requests
        )                
        ORDER BY RANDOM()
        LIMIT %s
    """, (n,))
    id_customers = cur.fetchall()

    for id_customer in id_customers:
        cur.execute("""
            SELECT id_route
            FROM routes
            ORDER BY RANDOM()
            LIMIT 1
        """)
        id_route = cur.fetchone()

        # Datos de ubicación
        pickup_location = destination_location = None
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
                pickup_location = {'x': pickup_checkpoint[0], 'y': pickup_checkpoint[1]}

            cur.execute("""
                SELECT location[0] as x, location[1] as y
                FROM checkpoint_routes
                WHERE id_route = %s
                ORDER BY checkpoint_number DESC
                LIMIT 1
            """, (id_route[0],))
            destination_checkpoint = cur.fetchone()
            if destination_checkpoint:
                destination_location = {'x': destination_checkpoint[0], 'y': destination_checkpoint[1]}

        customers_data.append({
            'id_customer': id_customer[0],
            'id_route': id_route[0] if id_route else None,
            'pickup_location': pickup_location,
            'destination_location': destination_location
        })

    cur.close()
    conn.close()

    return customers_data

def get_inactive_cars_and_drivers(n=5):
    conn = psycopg2.connect(
        dbname="DP2",
        user="postgres",
        password=password,
        host="34.38.87.73",
        port="5432"
    )
    cur = conn.cursor()
    drivers_data = []

    # Obtener n conductores inactivos
    cur.execute("""
        SELECT id_driver
        FROM drivers
        WHERE driver_status = 'Inactive'
        AND id_driver NOT IN (
            SELECT id_driver
            FROM active_vehicles
        )                
        ORDER BY RANDOM()
        LIMIT %s
    """, (n,))
    id_drivers = cur.fetchall()

    for id_driver in id_drivers:
        cur.execute("""
            SELECT id_route
            FROM routes
            ORDER BY RANDOM()
            LIMIT 1
        """)
        id_route = cur.fetchone()

        # Datos de ubicación
        pickup_location = destination_location = None
        if id_route:
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
            locations = cur.fetchall()
            if locations:
                pickup_location = {'x': float(locations[0][0].replace('(', '').replace(')', '').split(',')[0]),
                                   'y': float(locations[0][0].replace('(', '').replace(')', '').split(',')[1])}
                destination_location = {'x': float(locations[1][0].replace('(', '').replace(')', '').split(',')[0]),
                                        'y': float(locations[1][0].replace('(', '').replace(')', '').split(',')[1])}

        drivers_data.append({
            'id_driver': id_driver[0],
            'id_route': id_route[0] if id_route else None,
            'pickup_location': pickup_location,
            'destination_location': destination_location
        })

    cur.close()
    conn.close()

    return drivers_data

def publish_message(project_id, topic_id, message):
    """Publica un mensaje a un tópico de Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = message.encode("utf-8")

    try:
        publish_future = publisher.publish(topic_path, data)
        publish_future.result()
        print(f"Message published: {message}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def send_new_active_vehicles():
    while True:
        customers_data = get_inactive_customers(5)
        drivers_data = get_inactive_cars_and_drivers(5)

        message = {
            'customers': customers_data,
            'drivers': drivers_data
        }
        message_json = json.dumps(message)

        publish_message('edem-dp2', 'all_data', message_json)

        time.sleep(2)

send_new_active_vehicles()
