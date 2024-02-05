import psycopg2
import openpyxl
import pandas as pd
from faker import Faker
from math import radians,cos,sin,asin,sqrt
from XX_Creacion_de_clases import car,driver,customer, BaseDeDatos


#REGISTROS A CREAR:
numero_registros_coches_y_conductores = 1
numero_clientes = 40
db = BaseDeDatos()

#FUNCIONES DE INSERCIÓN:

def calculate_distance(lon1, lat1, lon2, lat2):
    # Convertir a radianes
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Fórmula de Haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))

    # Radio de la Tierra en metros
    r = 6371000
    return c * r

def insert_cars_data():
    num_records_to_generate = numero_registros_coches_y_conductores
    for _ in range(num_records_to_generate):
        car_instance = car()
        db.ejecutar("""
            INSERT INTO cars (id_driver, brand, model, car_seats, dissability_readyness)
            VALUES (%s, %s, %s, %s, %s)
        """, (car_instance.driver_id, car_instance.brand, car_instance.model, car_instance.seats, car_instance.dissability_readyness))
    print('Se han cargado datos de coches con éxito')

def insert_drivers_data():
    num_records_to_generate = numero_registros_coches_y_conductores
    for _ in range(num_records_to_generate):
        driver_instance = driver()
        db.ejecutar("""
            INSERT INTO drivers (name, surname, driver_license, driver_status)
            VALUES (%s, %s, %s, %s)
        """, (driver_instance.name, driver_instance.surname, driver_instance.driver_license, driver_instance.status))
    print('Se han cargado datos de conductores con éxito')

def insert_customers_data():
    num_records_to_generate = numero_clientes
    for _ in range(num_records_to_generate):
        customer_instance = customer()
        db.ejecutar("""
            INSERT INTO customers (name, surname, email)
            VALUES (%s, %s, %s)
        """, (customer_instance.name, customer_instance.surname, customer_instance.email))
    print('Se han cargado datos de clientes con éxito')

def insert_routes_and_checkpoints(excel_file):
    df = pd.read_excel(excel_file)
    checkpoints_count = df.groupby('id_route')['checkpoint_number'].nunique().reset_index(name='total_checkpoints')
    alias_route = df[['id_route', 'Alias']].drop_duplicates().set_index('id_route')['Alias']

    for id_route in checkpoints_count['id_route']:
        total_checkpoints = checkpoints_count.loc[checkpoints_count['id_route'] == id_route, 'total_checkpoints'].iloc[0]
        alias = alias_route.loc[id_route]

        db.ejecutar("""
            INSERT INTO routes (id_route, alias, total_checkpoints)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_route) DO UPDATE SET
            alias = EXCLUDED.alias,
            total_checkpoints = EXCLUDED.total_checkpoints
        """, (id_route, alias, total_checkpoints))

    print('Se han cargado datos de rutas con éxito')

    # Insertar checkpoints y calcular distancia
    df.sort_values(by=['id_route', 'checkpoint_number'], inplace=True)
    last_route = None
    last_coord = None
    for _, row in df.iterrows():
        coord_parts = row['coordenates'].strip().split(',')
        x, y = map(float, coord_parts[:2])
        distance = 0
        if last_route == row['id_route'] and last_coord:
            distance = calculate_distance(last_coord[0], last_coord[1], x, y)
        db.ejecutar("""
            INSERT INTO checkpoint_routes (id_route, checkpoint_number, location, distance_previous_checkpoint)
            VALUES (%s, %s, POINT(%s, %s), %s)
        """, (row['id_route'], row['checkpoint_number'], x, y, distance))
        last_route = row['id_route']
        last_coord = (x, y)

    print("Se han cargado datos de checkpoint de rutas con éxito.")

db.cerrar()

def insert_all_data():
    insert_drivers_data()
    insert_cars_data()
    insert_customers_data()
    insert_routes_and_checkpoints(r'G:\Mi unidad\PYTHON\DP21\Maestro_Rutas_definitivo.xlsx')
    print ("Todos los datos se han generado bien")