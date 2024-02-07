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

def actualizar_solicitud(db, id_request, nuevo_pick_up_point, nuevo_estado, nuevo_request_time_end, nuevo_checkpoint):
    # Solo actualizamos el estado a 'Missed' si es necesario, de lo contrario, mantenemos el estado actual
    parametros = (nuevo_pick_up_point, nuevo_request_time_end.strftime("%Y-%m-%d %H:%M:%S"), nuevo_checkpoint, id_request)
    if nuevo_estado == 'Missed':
        sql = """
            UPDATE ride_requests
            SET pick_up_point = %s, request_status = 'Missed', request_time_end = %s, current_checkpoint = %s
            WHERE id_request = %s
        """
    else:
        sql = """
            UPDATE ride_requests
            SET pick_up_point = %s, request_time_end = %s, current_checkpoint = %s
            WHERE id_request = %s
        """
    db.ejecutar(sql, parametros)

def encontrar_siguiente_checkpoint(db, id_route, current_checkpoint):
    total_checkpoints_result = db.consultar("""
        SELECT total_checkpoints
        FROM routes
        WHERE id_route = %s
    """, (id_route,))
    total_checkpoints = total_checkpoints_result[0][0] if total_checkpoints_result else 0

    siguiente_checkpoint = current_checkpoint + 1

    if siguiente_checkpoint <= total_checkpoints:
        nueva_posicion_result = db.consultar("""
            SELECT location
            FROM checkpoint_routes
            WHERE id_route = %s AND checkpoint_number = %s
        """, (id_route, siguiente_checkpoint))
        nueva_posicion = nueva_posicion_result[0][0] if nueva_posicion_result else None
        es_ultimo_checkpoint = siguiente_checkpoint == total_checkpoints
        return nueva_posicion, siguiente_checkpoint, es_ultimo_checkpoint
    else:
        # Si ya no hay más checkpoints, se considera el último y se devuelve el estado correspondiente
        return None, current_checkpoint, True  # Este True indica que ya se considera el último para efectos de cambio de estado
    
def actualizar_estado_cliente_por_solicitud_missed(db, id_request):
    cliente_result = db.consultar("""
        SELECT id_customer 
        FROM ride_requests 
        WHERE id_request = %s AND request_status = 'Missed'
    """, (id_request,))
    
    for (id_customer,) in cliente_result:
            db.ejecutar("""
                UPDATE customers
                SET customer_status = 'Inactive'
                WHERE id_customer = %s
            """, (id_customer,))
            print(f"Cliente {id_customer} asociado al request {id_request} ha sido actualizado a 'Inactive'.")

def procesar_solicitudes_activas():
    db = BaseDeDatos()
    while True:
        solicitudes_activas = obtener_solicitudes_activas(db)
        for solicitud in solicitudes_activas:
            id_request, request_time_end, pick_up_point, id_route, request_status, current_checkpoint = solicitud
            now = datetime.now()
            if (request_time_end + timedelta(seconds=30)) < now:
                nuevo_pick_up_point, nuevo_checkpoint, es_ultimo_checkpoint = encontrar_siguiente_checkpoint(db, id_route, current_checkpoint)
                
                # Actualizar siempre la solicitud con el nuevo checkpoint y pick_up_point, si es aplicable
                estado_actualizacion = 'Missed' if es_ultimo_checkpoint else request_status
                
                # Nota: Asumimos que encontrar_siguiente_checkpoint devuelve None para nuevo_pick_up_point si es_ultimo_checkpoint y no hay una nueva posición
                actualizar_solicitud(db, id_request, nuevo_pick_up_point if nuevo_pick_up_point else pick_up_point, estado_actualizacion, now, nuevo_checkpoint)

                if es_ultimo_checkpoint:
                    print(f"Solicitud {id_request} actualizada a Missed. Checkpoint final alcanzado.")
                    actualizar_estado_cliente_por_solicitud_missed(db, id_request)
                else:
                    print(f"Solicitud {id_request} actualizada. Posición: {nuevo_pick_up_point if nuevo_pick_up_point else 'Sin cambio'}, Checkpoint: {nuevo_checkpoint}.")

        time.sleep(20)

if __name__ == "__main__":
    procesar_solicitudes_activas()