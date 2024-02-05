from XX_Creacion_de_clases import BaseDeDatos
from datetime import datetime
from random import randint
import time

#ESTE SCRIP ACTUALIZA LAS POSICIONES DE LOS VEHÍCULOS Y EL CIERRE DE LOS VIAJES (CUANDO TERMINAN). TAMBIÉN CREA EL REGISTRO EN RATING.

def obtener_vehiculos_activos(db):
    return db.consultar("""
        SELECT id_service_offer, current_checkpoint, id_route, id_driver, service_status, time_end
        FROM active_vehicles
        WHERE service_status != 'Ended'
    """)

def actualizar_estado_vehiculo(db, id_vehiculo, nuevo_estado, nuevo_tiempo_end):
    db.ejecutar("""
        UPDATE active_vehicles
        SET service_status = %s, time_end = %s
        WHERE id_service_offer = %s
    """, (nuevo_estado, nuevo_tiempo_end, id_vehiculo))

def actualizar_estado_conductor(db, id_driver, nuevo_estado):
    db.ejecutar("""
        UPDATE drivers
        SET driver_status = %s
        WHERE id_driver = %s
    """, (nuevo_estado, id_driver))

def actualizar_posicion_vehiculo(db, id_vehiculo, id_route, nuevo_checkpoint):
    nueva_posicion = db.consultar("""
        SELECT location
        FROM checkpoint_routes
        WHERE id_route = %s AND checkpoint_number = %s
    """, (id_route, nuevo_checkpoint))[0][0]
    db.ejecutar("""
        UPDATE active_vehicles
        SET current_checkpoint = %s, current_position = %s
        WHERE id_service_offer = %s
    """, (nuevo_checkpoint, nueva_posicion, id_vehiculo))

def actualizar_ride(db, id_vehiculo, nuevo_estado_ride, nuevo_tiempo_end):
    id_service_offer = db.consultar("""
        SELECT id_service_offer
        FROM active_vehicles
        WHERE id_service_offer = %s
    """, (id_vehiculo,))
    if id_service_offer:
        id_service_offer = id_service_offer[0][0]
        db.ejecutar("""
            UPDATE rides
            SET ride_status = %s, ride_time_end = %s
            WHERE id_service_offer = %s AND ride_status != 'Ended'
        """, (nuevo_estado_ride, nuevo_tiempo_end, id_service_offer))
        if nuevo_estado_ride == 'Ended':
                resultado = db.consultar("""
                    SELECT id_ride, id_customer
                    FROM rides
                    WHERE id_service_offer = %s
                """, (id_vehiculo,))
                if resultado:
                    id_ride, id_customer = resultado[0]
                    rating = randint(1, 10)
                    db.ejecutar("""
                        INSERT INTO Rating (id_ride, id_customer, rating)
                        VALUES (%s, %s, %s)
                    """, (id_ride, id_customer, rating))
                db.ejecutar("""
                    UPDATE customers
                    SET customer_status = 'Inactive'
                    WHERE id_customer = %s
                """, (id_customer,))

def finalizar_o_avanzar_vehiculo(db, vehiculo):
    id_vehiculo, current_checkpoint, id_route, id_driver, service_status, time_end = vehiculo
    now = datetime.now()
    time_difference = now - time_end
    if time_difference.total_seconds() > 40:
        total_checkpoints = db.consultar("""
            SELECT total_checkpoints
            FROM routes
            WHERE id_route = %s
        """, (id_route,))[0][0]

        nuevo_checkpoint = current_checkpoint + 1
        nueva_posicion = db.consultar("""
            SELECT location
            FROM checkpoint_routes
            WHERE id_route = %s AND checkpoint_number = %s
        """, (id_route, nuevo_checkpoint))[0][0]

        if nuevo_checkpoint > total_checkpoints:
            print(f"Error: El checkpoint actual {nuevo_checkpoint} excede el total para la ruta {id_route}.")
            return

        db.ejecutar("""
            UPDATE active_vehicles
            SET current_checkpoint = %s, current_position = %s, time_end = %s
            WHERE id_service_offer = %s
        """, (nuevo_checkpoint, nueva_posicion, now.strftime("%Y-%m-%d %H:%M:%S"), id_vehiculo))

        if nuevo_checkpoint == total_checkpoints:
            actualizar_estado_vehiculo(db, id_vehiculo, 'Ended', now.strftime("%Y-%m-%d %H:%M:%S"))
            actualizar_estado_conductor(db, id_driver, 'Inactive')
            actualizar_ride(db, id_vehiculo, 'Ended', now.strftime("%Y-%m-%d %H:%M:%S"))

            # Actualizar el campo "ride_current_position" en la tabla "rides"
            db.ejecutar("""
                UPDATE rides
                SET ride_current_position = %s
                WHERE id_service_offer = %s
            """, (nueva_posicion, id_vehiculo))

            print(f"El viaje con service offer {id_vehiculo} se ha actualizado y cerrado")
        else:
            actualizar_estado_vehiculo(db, id_vehiculo, 'Active', now.strftime("%Y-%m-%d %H:%M:%S"))
            actualizar_ride(db, id_vehiculo, 'Active', now.strftime("%Y-%m-%d %H:%M:%S"))

            # Actualizar el campo "ride_current_position" en la tabla "rides"
            db.ejecutar("""
                UPDATE rides
                SET ride_current_position = %s
                WHERE id_service_offer = %s
            """, (nueva_posicion, id_vehiculo))

            print(f"El viaje con service offer {id_vehiculo} se ha actualizado")

def procesar_vehiculos_activos():
    db = BaseDeDatos()
    while True:
        vehiculos_activos = obtener_vehiculos_activos(db)
        for vehiculo in vehiculos_activos:
            finalizar_o_avanzar_vehiculo(db, vehiculo)
        print ("Todos los coches actives se han actualizado")
        time.sleep (10)

if __name__ == "__main__":
    procesar_vehiculos_activos()

procesar_vehiculos_activos()   