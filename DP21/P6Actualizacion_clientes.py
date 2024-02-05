from XX_Creacion_de_clases import BaseDeDatos
from datetime import datetime, timedelta
from random import randint
import time

from XX_Creacion_de_clases import BaseDeDatos
from datetime import datetime, timedelta
import time

def obtener_solicitudes_activas(db):
    return db.consultar("""
        SELECT id_request, request_time_end, pick_up_point, id_route, request_status, current_checkpoint
        FROM ride_requests
        WHERE request_status = 'Waiting'
    """)

# Actualizar para incluir 'nuevo_checkpoint' como argumento
def actualizar_solicitud(db, id_request, nuevo_pick_up_point, nuevo_estado, nuevo_request_time_end, nuevo_checkpoint):
    db.ejecutar("""
        UPDATE ride_requests
        SET pick_up_point = %s, request_status = %s, request_time_end = %s, current_checkpoint = %s
        WHERE id_request = %s
    """, (nuevo_pick_up_point, nuevo_estado, nuevo_request_time_end, nuevo_checkpoint, id_request))

def encontrar_siguiente_checkpoint(db, id_route, current_checkpoint):
    total_checkpoints_result = db.consultar("""
        SELECT total_checkpoints
        FROM routes
        WHERE id_route = %s
    """, (id_route,))
    total_checkpoints = total_checkpoints_result[0][0] if total_checkpoints_result else 0

    es_ultimo_checkpoint = current_checkpoint >= total_checkpoints

    if not es_ultimo_checkpoint:
        siguiente_checkpoint = current_checkpoint + 1
        nueva_posicion_result = db.consultar("""
            SELECT location
            FROM checkpoint_routes
            WHERE id_route = %s AND checkpoint_number = %s
        """, (id_route, siguiente_checkpoint))
        nueva_posicion = nueva_posicion_result[0][0] if nueva_posicion_result else None
        return nueva_posicion, siguiente_checkpoint, False
    else:
        return None, current_checkpoint, True

def procesar_solicitudes_activas():
    db = BaseDeDatos()
    while True:
        solicitudes_activas = obtener_solicitudes_activas(db)
        for solicitud in solicitudes_activas:
            id_request, request_time_end, pick_up_point, id_route, request_status, current_checkpoint = solicitud
            now = datetime.now()
            if (request_time_end + timedelta(seconds=20)) < now:
                nuevo_pick_up_point, nuevo_checkpoint, es_ultimo_checkpoint = encontrar_siguiente_checkpoint(db, id_route, current_checkpoint)
                if not es_ultimo_checkpoint:
                    actualizar_solicitud(db, id_request, nuevo_pick_up_point, 'Waiting', now.strftime("%Y-%m-%d %H:%M:%S"), nuevo_checkpoint)
                else:
                    actualizar_solicitud(db, id_request, pick_up_point, "Missed", now.strftime("%Y-%m-%d %H:%M:%S"), current_checkpoint)
                print(f"Solicitud {id_request} actualizada. Posición: {nuevo_pick_up_point if nuevo_pick_up_point else pick_up_point}, Estado: {'Missed' if es_ultimo_checkpoint else 'Waiting'}.")
        time.sleep(20)

if __name__ == "__main__":
    procesar_solicitudes_activas()