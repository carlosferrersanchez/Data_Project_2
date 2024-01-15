import psycopg2
import pandas as pd
from faker import Faker
from DP2_Creacion_de_clases import car, driver, customer

def insert_cars_data():
    conn = psycopg2.connect(
        dbname = "DB_DP2",
        user = "dp2",
        password = "dp2",
        host = "localhost", #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
        port = "5432"
    )
    cur = conn.cursor()
    num_records_to_generate = 10
    for _ in range(num_records_to_generate):
        car_instance = car()
        cur.execute("""
            INSERT INTO cars (brand, model, car_seats, car_status, dissability_readyness)
            VALUES (%s, %s, %s, %s, %s)
        """, (car_instance.brand, car_instance.model, car_instance.seats, car_instance.status, car_instance.dissability_readyness))

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
    num_records_to_generate = 10
    for _ in range(num_records_to_generate):
        driver_instance = driver()
        cur.execute("""
            INSERT INTO drivers (name, surname, driver_license)
            VALUES (%s, %s, %s)
        """, (driver_instance.name, driver_instance.surname, driver_instance.driver_license))
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
    num_records_to_generate = 5
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
def insert_routes():
    # Conectar a la base de datos
    conn = psycopg2.connect(
        dbname="DB_DP2",
        user="dp2",
        password="dp2",
        host="localhost",
        port="5432"
    )

    # Crear un cursor
    cur = conn.cursor()

    # Insertar un valor manualmente en la tabla routes
    id_route = 1  # Cambia este valor según tu necesidad
    alias = "Mi Ruta"  # Cambia este valor según tu necesidad

    # Ejecutar la consulta SQL para insertar el valor
    cur.execute("""
        INSERT INTO routes (id_route, alias)
        VALUES (%s, %s)
    """, (id_route, alias))

    # Confirmar la transacción
    conn.commit()

    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()
    print ('Se han cargado las rutas éxito')
def insert_checkpoint():
    conn = psycopg2.connect(
        dbname = "DB_DP2",
        user = "dp2",
        password = "dp2",
        host = "localhost", #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
        port = "5432"
    )
    cur = conn.cursor()
    
    df = pd.read_excel(r'G:\Mi unidad\PYTHON\DP2\Otros\Maestro_Rutas.xlsx')

    for _, row in df.iterrows():
        coordenates = f"({row['coordenates'].strip().rstrip(',0')})"
        cur.execute("""
            INSERT INTO checkpoint_routes (id_route, checkpoint_number, location)
            VALUES (%s, %s, %s::POINT)
        """, (row['id_route'], row['checkpoint_number'], coordenates))
    conn.commit()
    cur.close()
    conn.close()
    print("Datos de checkpoints insertados con éxito.")

insert_cars_data()
insert_drivers_data()
insert_customers_data()
insert_routes ()
insert_checkpoint ()
print ("Todos los datos se han generado bien")