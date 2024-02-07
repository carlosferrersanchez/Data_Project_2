from XX_Creacion_de_clases import BaseDeDatos
from datetime import datetime
import random
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

def actualizar_posicion_vehiculo(db, id_vehiculo, nuevo_checkpoint, nueva_posicion, now):
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")  # Formato de timestamp para SQL
    db.ejecutar("""
        UPDATE active_vehicles
        SET current_checkpoint = %s, current_position = %s, time_end = %s
        WHERE id_service_offer = %s
    """, (nuevo_checkpoint, nueva_posicion, now_str, id_vehiculo))

def actualizar_ride(db, id_vehiculo, now, nueva_posicion, finalizado=False):
    # Actualiza todos los rides asociados al service_offer
    nuevo_estado_ride = 'Ended' if finalizado else 'Ongoing'
    db.ejecutar("""
        UPDATE rides
        SET ride_time_end = %s, ride_current_position = %s, ride_status = %s
        WHERE id_service_offer = %s AND ride_status != 'Ended'
    """, (now.strftime("%Y-%m-%d %H:%M:%S"), nueva_posicion, nuevo_estado_ride, id_vehiculo))
    if finalizado:
        generar_rating_para_rides_finalizados(db, id_vehiculo)

def generar_rating_para_rides_finalizados(db, id_vehiculo):
    rides = db.consultar("""
        SELECT id_ride, id_customer FROM rides WHERE id_service_offer = %s AND ride_status = 'Ended'
    """, (id_vehiculo,))
    for id_ride, id_customer in rides:
        probabilities = [0.10, 0.15, 0.30, 0.35, 0.10] # Generación de rating aleatorio ponderado
        rating = random.choices(range(1, 6), probabilities)[0] 
        db.ejecutar("""
            INSERT INTO Rating (id_ride, id_customer, rating) VALUES (%s, %s, %s)
        """, (id_ride, id_customer, rating))

def actualizar_customer(db, id_vehiculo):
    # Obtiene todos los clientes asociados a los rides del service_offer finalizado
    clientes = db.consultar("""
        SELECT DISTINCT id_customer 
        FROM rides 
        WHERE id_service_offer = %s AND ride_status = 'Ended'
    """, (id_vehiculo,))
    
    if clientes:
        for id_customer, in clientes:
            # Actualiza el estado del cliente a 'Inactive'
            db.ejecutar("""
                UPDATE customers
                SET customer_status = 'Inactive'
                WHERE id_customer = %s
            """, (id_customer,))
        print(f"Clientes asociados al service offer {id_vehiculo} en Rides han sido actualizados a 'Inactive'.")
    else:
        print(f"No se encontraron clientes asociados en Rides para el service_offer {id_vehiculo}.")

def actualizar_checkpoint_y_posicion_en_active_vehicles(db, id_vehiculo, nuevo_checkpoint, nueva_posicion, now):
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    db.ejecutar("""
        UPDATE active_vehicles
        SET current_checkpoint = %s, current_position = %s, time_end = %s
        WHERE id_service_offer = %s
    """, (nuevo_checkpoint, nueva_posicion, now_str, id_vehiculo))

def finalizar_o_avanzar_vehiculo(db, vehiculo):
    id_vehiculo, current_checkpoint, id_route, id_driver, service_status, time_end = vehiculo
    now = datetime.now()
    time_difference = now - time_end
    if time_difference.total_seconds() > 2:
        total_checkpoints, = db.consultar("""
            SELECT total_checkpoints FROM routes WHERE id_route = %s
        """, (id_route,))[0]
        nuevo_checkpoint = current_checkpoint + 1 if current_checkpoint < total_checkpoints else current_checkpoint
        nueva_posicion, = db.consultar("""
            SELECT location FROM checkpoint_routes WHERE id_route = %s AND checkpoint_number = %s
        """, (id_route, nuevo_checkpoint))[0]

        if nuevo_checkpoint == total_checkpoints:
            actualizar_estado_vehiculo(db, id_vehiculo, 'Ended', now.strftime("%Y-%m-%d %H:%M:%S"))
            actualizar_estado_conductor(db, id_driver, 'Inactive')
            actualizar_checkpoint_y_posicion_en_active_vehicles(db, id_vehiculo, nuevo_checkpoint, nueva_posicion, now)
            actualizar_ride(db, id_vehiculo, now, nueva_posicion, finalizado=True)
            actualizar_customer(db, id_vehiculo)
            print(f"El viaje con service offer {id_vehiculo} ha finalizado.")
        else:
            actualizar_posicion_vehiculo(db, id_vehiculo, nuevo_checkpoint, nueva_posicion, now)  # Aquí pasamos `now`
            actualizar_ride(db, id_vehiculo, now, nueva_posicion)
            print(f"El viaje con service offer {id_vehiculo} se ha actualizado.")

def procesar_vehiculos_activos():
    db = BaseDeDatos()
    while True:
        vehiculos_activos = obtener_vehiculos_activos(db)
        for vehiculo in vehiculos_activos:
            finalizar_o_avanzar_vehiculo(db, vehiculo)
        print("Todos los coches activos se han actualizado.")

if __name__ == "__main__":
    procesar_vehiculos_activos()