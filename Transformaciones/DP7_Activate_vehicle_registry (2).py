from confluent_kafka import Consumer
import json
import psycopg2
from datetime import datetime

#ESTE SCRIPT CONSUME EL MENSAJE DEL CONDUCTOR, LO ACTIVA EN BBDD y GENERA UN REGISTRO CON SUS DATOS EN LA TABLA "ACTIVE_VEHICLES"
def update_driver_status_and_create_record(id_driver, id_route):
    try:
        conn = psycopg2.connect(
            dbname="DB_DP2",
            user="dp2",
            password="dp2",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        cur.execute("UPDATE drivers SET driver_status = 'Active' WHERE id_driver = %s", (id_driver,))
        
        cur.execute("""
            SELECT location 
            FROM checkpoint_routes 
            WHERE id_route = %s AND checkpoint_number = 1
        """, (id_route,))
        location_result = cur.fetchone()
        current_position = location_result[0] if location_result else None

        cur.execute("SELECT car_seats FROM cars WHERE id_driver = %s", (id_driver,))
        seats_available = cur.fetchone()[0]

        time_start = datetime.now()
        time_end = time_start

        cur.execute("""
            INSERT INTO active_vehicles (id_driver, id_route, time_start, time_end, current_position, seats_available, service_status)
            VALUES (%s, %s, %s, %s, %s, %s, 'Active')
        """, (id_driver, id_route, time_start, time_end, current_position, seats_available))

        conn.commit()
        cur.close()
        conn.close()
        print(f"Se ha cambiado el estado del conductor con id: {id_driver}. Ahora está Activo.")
    except Exception as e:
        print(f"Error al actualizar la base de datos para el conductor {id_driver}: {e}")

  

# Función principal para consumir mensajes de Kafka y actualizar la base de datos

def consume_and_update():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-consumer-group-1222',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(config)
    consumer.subscribe(['New_active_vehicle'])
    
    try:
        while True:
            msg = consumer.poll(1.0)  # Espera de 1 segundo
            if msg is None:
                continue
            if msg.error():
                print("Error del consumidor: {}".format(msg.error()))
                continue
            message_data = json.loads(msg.value().decode('utf-8'))
            id_driver = message_data.get('id_driver')
            id_route = message_data.get('id_route')
            if id_driver and id_route:
                update_driver_status_and_create_record(id_driver, id_route)
    finally:
        consumer.close()

consume_and_update()