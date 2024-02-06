from XX_Creacion_de_clases import BaseDeDatos
from datetime import datetime
from google.cloud import pubsub_v1
from getpass import getpass
import random
import json
import time
import psycopg2
import numpy as np

#PRIMERO INTRODUCIR ESTO EN TERMINAL PARA AUTENTICARTE: gcloud auth application-default login

def generate_customer_requests():
    db = BaseDeDatos()
    while True:
        id_customer_result = db.consultar("""
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

        if id_customer_result:
            id_customer = id_customer_result[0][0]

            id_route_result = db.consultar("""
                SELECT id_route
                FROM routes
                ORDER BY RANDOM()
                LIMIT 1
            """)
            id_route = id_route_result[0][0] if id_route_result else None

            pickup_checkpoint_result = db.consultar("""
                SELECT location, checkpoint_number
                FROM checkpoint_routes
                WHERE id_route = %s
                ORDER BY RANDOM()
                LIMIT 1
            """, (id_route,))
            destination_checkpoint_result = db.consultar("""
                SELECT location, checkpoint_number
                FROM checkpoint_routes
                WHERE id_route = %s
                ORDER BY checkpoint_number DESC
                LIMIT 1
            """, (id_route,))

            pickup_location, pickup_checkpoint_number = pickup_checkpoint_result[0]
            destination_location, destination_checkpoint_number = destination_checkpoint_result[0]

            total_distance_result = db.consultar("""
                SELECT SUM(distance_previous_checkpoint)
                FROM checkpoint_routes
                WHERE id_route = %s AND checkpoint_number BETWEEN %s AND %s
            """, (id_route, pickup_checkpoint_number, destination_checkpoint_number))

            total_distance = total_distance_result[0][0] if total_distance_result and total_distance_result[0][0] is not None else 0
            total_distance_km = total_distance / 1000
            price = max(round(total_distance_km * 5, 2), 3.99) #No es tarifa real, pero ajustada a duración ruta para mejor similitud a realidad.
            probabilidades = [0.35, 0.35, 0.20, 0.10]
            passengers = int(np.random.choice([1, 2, 3, 4], p=probabilidades))

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            consulta_sql_insert_customer_request = """
                INSERT INTO ride_requests (
                    id_customer, date, pick_up_point, drop_point, request_status, 
                    request_time_start, request_time_end, id_route, price, passengers, distance_covered, current_checkpoint
                )
                VALUES (%s, %s, %s, %s, 'Waiting', %s::timestamp, %s::timestamp, %s, %s, %s, %s, %s)
            """
            parametros_insert_customer_request = (
                id_customer, 
                datetime.now(),
                pickup_location,
                destination_location,
                now_str,  
                now_str,  
                id_route,
                price,
                passengers,
                total_distance_km,
                pickup_checkpoint_number,
            )
            db.ejecutar(consulta_sql_insert_customer_request, parametros_insert_customer_request)

            print(f"Ride request created for customer {id_customer} with price: {price}, passengers: {passengers}, and current_checkpoint: {pickup_checkpoint_number}")

            db.ejecutar("""
                UPDATE customers
                SET customer_status = 'Active'
                WHERE id_customer = %s
            """, (id_customer,))
        else:
            print("Todos los clientes están activos")

        time.sleep(15)

if __name__ == "__main__":
    generate_customer_requests()



generate_customer_requests()