import psycopg2
import openpyxl
import pandas as pd
from faker import Faker
from math import radians,cos,sin,asin,sqrt
from DP2_Creacion_de_clases import car, driver, customer

#IMPORTANTE!! Como un conductor solo puede tener un coche, el número de registros a crear de coches y conductores tiene que ser el mismo
def insert_cars_data():
    conn = psycopg2.connect(
        dbname = "DB_DP2",
        user = "dp2",
        password = "dp2",
        host = "localhost", #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
        port = "5432"
    )
    cur = conn.cursor()
    num_records_to_generate = 40
    for _ in range(num_records_to_generate):
        car_instance = car()
        cur.execute("""
            INSERT INTO cars (id_driver, brand, model, car_seats, dissability_readyness)
            VALUES (%s, %s, %s, %s, %s)
        """, (car_instance.driver_id, car_instance.brand, car_instance.model, car_instance.seats, car_instance.dissability_readyness))

    conn.commit()
    cur.close()
    conn.close()
    print ('Se han cargado datos de coches con éxito')
    
def insert_drivers_data():
    conn = psycopg2.connect(
        dbname = "DB_DP2",
        user = "dp2",
        password = "dp2",
        host = "localhost", # Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
        port = "5432"
    )
    cur = conn.cursor()
    num_records_to_generate = 40
    for _ in range(num_records_to_generate):
        driver_instance = driver()
        cur.execute("""
            INSERT INTO drivers (name, surname, driver_license, driver_status)
            VALUES (%s, %s, %s, %s)
        """, (driver_instance.name, driver_instance.surname, driver_instance.driver_license, driver_instance.status))
    conn.commit()
    cur.close()
    conn.close()
    print ('Se han cargado datos de conductores con éxito')

def insert_customers_data():
    conn = psycopg2.connect(
        dbname = "DB_DP2",
        user = "dp2",
        password = "dp2",
        host = "localhost", #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
        port = "5432"
    )
    cur = conn.cursor()
    num_records_to_generate = 200
    for _ in range(num_records_to_generate):
        customer_instance = customer()
        cur.execute("""
            INSERT INTO customers (name, surname, email)
            VALUES (%s, %s, %s)
        """, (customer_instance.name, customer_instance.surname, customer_instance.email))
    conn.commit()
    cur.close()
    conn.close()
    print ('Se han cargado datos de clientes con éxito')

def insert_routes(excel_file):
    conn = psycopg2.connect(
        dbname="DB_DP2",
        user="dp2",
        password="dp2",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    df = pd.read_excel(excel_file)
    checkpoints_count = df.groupby('id_route')['checkpoint_number'].nunique().reset_index(name='total_checkpoints')
    
    alias_route = df[['id_route', 'Alias']].drop_duplicates().set_index('id_route')['Alias']

    for id_route in checkpoints_count['id_route']:
        total_checkpoints = checkpoints_count.loc[checkpoints_count['id_route'] == id_route, 'total_checkpoints'].iloc[0].item()  # Convertir a entero nativo de Python
        alias = alias_route[id_route].strip()

        cur.execute("""
            INSERT INTO routes (id_route, alias, total_checkpoints)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_route) DO UPDATE SET
            alias = EXCLUDED.alias,
            total_checkpoints = EXCLUDED.total_checkpoints
        """, (id_route, alias, total_checkpoints))

    conn.commit()
    cur.close()
    conn.close()
    print ('Se han cargado datos de rutas con éxito')

def insert_checkpoint(excel_file):
    def calculate_distance(lon1, lat1, lon2, lat2):
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371000
        return c * r
    def update_total_distance (conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT id_route, SUM(distance_previous_checkpoint) as total_distance
            FROM checkpoint_routes
            GROUP BY id_route
        """)
        route_distances = cur.fetchall()
        for id_route, total_distance in route_distances:
            if total_distance is not None:
                cur.execute("""
                    UPDATE routes
                    SET total_distance = %s
                    WHERE id_route = %s
                """, (total_distance, id_route))
        conn.commit()
        cur.close()

    conn = psycopg2.connect(
        dbname = "DB_DP2",
        user = "dp2",
        password = "dp2",
        host = "localhost", #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
        port = "5432"
    )
    cur = conn.cursor()
    
    df = pd.read_excel(excel_file)
    df = df.sort_values(by=['id_route', 'checkpoint_number'])
    
    last_coord = None
    last_route = None

    for _, row in df.iterrows():
        coord_parts = row['coordenates'].strip().split(',')
        x, y = map(float, coord_parts[:2])
        coordinates = (x, y)
        if last_route == row['id_route'] and last_coord is not None:
            distance = calculate_distance(last_coord[0], last_coord[1], x, y)
        else:
            distance = 0

        cur.execute("""
            INSERT INTO checkpoint_routes (id_route, checkpoint_number, location, distance_previous_checkpoint)
            VALUES (%s, %s, POINT(%s, %s), %s)
        """, (row['id_route'], row['checkpoint_number'], x, y, distance))

        last_coord = coordinates
        last_route = row['id_route']
    
    update_total_distance(conn)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Se han cargado datos de checkpoint de rutas con éxito.")

#IMPORTANTE, PARA RESPETAR LAS RELACIONS DE TABLA SE DEBEN CREAR EN ESTE ORDEN
insert_drivers_data()
insert_cars_data()
insert_customers_data()
insert_routes(r'G:\Mi unidad\PYTHON\DP2\Otros\Maestro_Rutas.xlsx')
insert_checkpoint(r'G:\Mi unidad\PYTHON\DP2\Otros\Maestro_Rutas.xlsx')

print ("Todos los datos se han generado bien")