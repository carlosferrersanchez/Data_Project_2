import psycopg2
from psycopg2 import sql

def table_creation():
    dbname = "DB_DP2"
    user = "dp2"
    password = "dp2"
    host = "localhost" #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
    port = "5432"
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS car_type (
            ID_Type_car SERIAL PRIMARY KEY
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id_car SERIAL PRIMARY KEY,
            id_type_car INTEGER,
            model VARCHAR(25),
            car_status VARCHAR(20),
            FOREIGN KEY (id_type_car) REFERENCES car_type(ID_Type_car)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            id_driver SERIAL PRIMARY KEY,
            name VARCHAR(20),
            surname VARCHAR(20),
            driving_license VARCHAR(20)
            )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS routes (
            id_route SERIAL PRIMARY KEY,
            alias VARCHAR(25)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS checkpoint_routes (
            id_route INTEGER NOT NULL,
            checkpoint_number INTEGER NOT NULL,
            location POINT,
            FOREIGN KEY (id_route) REFERENCES routes(id_route)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_vehicles (
            id_service_offer SERIAL PRIMARY KEY,
            id_car INT,
            id_driver INT,
            id_route INT,
            date TIMESTAMP,
            current_position POINT,
            seats_available INT,
            FOREIGN KEY (id_car) REFERENCES cars(id_car),
            FOREIGN KEY (id_driver) REFERENCES drivers(id_driver),
            FOREIGN KEY (id_route) REFERENCES routes(id_route)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id_customer SERIAL PRIMARY KEY,
            name VARCHAR(20),
            surname VARCHAR(20),
            email VARCHAR(20)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ride_requests (
            id_request SERIAL PRIMARY KEY,
            id_customer INT,
            date TIMESTAMP,
            pick_up_point POINT,
            drop_point POINT,
            price FLOAT,
            passengers INT,
            request_status VARCHAR(20),
            request_time TIMESTAMP,
            FOREIGN KEY (id_customer) REFERENCES customers(id_customer)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rides (
            id_ride SERIAL PRIMARY KEY,
            id_service_offer INT,
            id_request INT,
            id_car INT,
            id_driver INT,
            id_route INT,
            id_customer INT,
            date TIMESTAMP,
            pick_up_point POINT,
            drop_point POINT,
            price FLOAT,
            passengers INT,
            ride_time TIMESTAMP,
            status VARCHAR(20),
            current_position POINT,
            FOREIGN KEY (id_service_offer) REFERENCES active_vehicles(id_service_offer),
            FOREIGN KEY (id_request) REFERENCES ride_requests (id_request),
            FOREIGN KEY (id_car) REFERENCES cars(id_car),
            FOREIGN KEY (id_driver) REFERENCES drivers(id_driver),
            FOREIGN KEY (id_route) REFERENCES routes(id_route),
            FOREIGN KEY (id_customer) REFERENCES customers(id_customer)

                
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

table_creation()