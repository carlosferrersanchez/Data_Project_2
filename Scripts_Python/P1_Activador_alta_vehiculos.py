from XX_Creacion_de_clases import BaseDeDatos
from datetime import datetime
from google.cloud import pubsub_v1
from getpass import getpass
import json
import time
import psycopg2

#PRIMERO INTRODUCIR ESTO EN TERMINAL PARA AUTENTICARTE: gcloud auth application-default login
def generate_vehicle_activations():
    while True:
        db = BaseDeDatos()
        id_driver = db.consultar("""
            SELECT id_driver
            FROM drivers
            WHERE driver_status = 'Inactive'
            AND id_driver NOT IN (
                SELECT id_driver
                FROM active_vehicles
                WHERE service_status != 'Ended'
            )
            ORDER BY RANDOM()
            LIMIT 1
        """)

        id_route = db.consultar("""
            SELECT id_route
            FROM routes
            ORDER BY RANDOM()
            LIMIT 1
        """)
        
        if id_driver and id_route:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # CAMBIO DE ESTADO A ACTIVO
            consulta_sql_update_driver = """
                UPDATE drivers
                SET driver_status = 'Active'
                WHERE id_driver = %s
            """
            parametros_update_driver = (id_driver[0][0],)
            db.ejecutar(consulta_sql_update_driver, parametros_update_driver)

            # OBTENER POSICIÓN ACTUAL PARA REGISTRO
            consulta_sql_current_position = """
                SELECT location
                FROM checkpoint_routes
                WHERE id_route = %s AND checkpoint_number = 1
            """
            parametros_current_position = (id_route[0][0],)
            current_position = db.consultar(consulta_sql_current_position, parametros_current_position)

            # OBTENER NUMERO DE ASIENTOS PARA ESE COCHE
            consulta_sql_seats_available = """
                SELECT car_seats
                FROM cars
                WHERE id_driver = %s
            """
            parametros_seats_available = (id_driver[0][0],)
            seats_available = db.consultar(consulta_sql_seats_available, parametros_seats_available)

            # INSERTAMOS EL NUEVO REGISTRO
            consulta_sql_insert_active_vehicle = """
                INSERT INTO active_vehicles (id_driver, id_route, time_start, time_end, current_position, seats_available, service_status, current_checkpoint)
                VALUES (%s, %s, %s, %s, %s, %s, 'On route', '1')
            """
            parametros_insert_active_vehicle = (
                id_driver[0][0], 
                id_route[0][0],
                now_str,
                now_str,
                current_position[0][0] if current_position else None,  # Asegúrate de manejar correctamente si current_position está vacío
                seats_available[0][0] if seats_available else 0,  # Asumiendo un valor por defecto si no se encuentra
            )
            db.ejecutar(consulta_sql_insert_active_vehicle, parametros_insert_active_vehicle)
            db.cerrar()
            print (id_driver[0][0])
        else:
            print ("No quedan coches inactivos, todos los coches están activos")
            db.cerrar()
        
        time.sleep(20)

generate_vehicle_activations()  