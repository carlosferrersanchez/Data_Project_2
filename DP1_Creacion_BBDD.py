import psycopg2
from psycopg2 import sql

def table_creation():
    dbname = "DB_DP2"
    user = "dp2"
    password = "dp2"
    host = "localhost" #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres" y "localhost" si va por fuera.
    port = "5432"
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            id_driver SERIAL PRIMARY KEY,
            name VARCHAR(50),
            surname VARCHAR(50),
            driver_license INT,
            driver_status VARCHAR (50)
            )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id_driver INT,
            brand VARCHAR(50),
            model VARCHAR(50),
            car_seats INT,
            dissability_readyness BOOLEAN,
            FOREIGN KEY (id_driver) REFERENCES drivers(id_driver)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS routes (
            id_route INT PRIMARY KEY,
            alias VARCHAR(50),
            total_checkpoints INT,
            total_distance FLOAT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS checkpoint_routes (
            id_route INT NOT NULL,
            checkpoint_number INT NOT NULL,
            location POINT,
            distance_previous_checkpoint FLOAT,
            FOREIGN KEY (id_route) REFERENCES routes(id_route)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_vehicles (
            id_service_offer SERIAL PRIMARY KEY,
            id_driver INT,
            id_route INT,
            time_start TIMESTAMP,
            time_end TIMESTAMP,
            current_position POINT,
            seats_available INT,
            service_status VARCHAR(50),
            FOREIGN KEY (id_driver) REFERENCES drivers(id_driver),
            FOREIGN KEY (id_route) REFERENCES routes(id_route)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id_customer SERIAL PRIMARY KEY,
            name VARCHAR(50),
            surname VARCHAR(50),
            email VARCHAR(50)
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
            request_status VARCHAR(50),
            request_time_start TIMESTAMP,
            request_time_end TIMESTAMP,
            FOREIGN KEY (id_customer) REFERENCES customers(id_customer)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rides (
            id_ride SERIAL PRIMARY KEY,
            id_service_offer INT,
            id_request INT,            
            id_driver INT,
            id_route INT,
            id_customer INT,
            real_pick_up_point POINT,
            real_drop_point POINT,
            price FLOAT,
            passengers INT,
            ride_time_start TIMESTAMP,
            ride_time_end TIMESTAMP,
            ride_status VARCHAR(50),
            ride_current_position POINT,
            FOREIGN KEY (id_service_offer) REFERENCES active_vehicles(id_service_offer),
            FOREIGN KEY (id_request) REFERENCES ride_requests (id_request),
            FOREIGN KEY (id_driver) REFERENCES drivers(id_driver),
            FOREIGN KEY (id_route) REFERENCES routes(id_route),
            FOREIGN KEY (id_customer) REFERENCES customers(id_customer)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rating (
            id_rating SERIAL PRIMARY KEY,
            id_ride INT,
            id_customer INT,
            rating INT,
            FOREIGN KEY (id_ride) REFERENCES rides(id_ride),
            FOREIGN KEY (id_customer) REFERENCES customers (id_customer)
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print ("Todas las tablas se han creado satisfactoriamente")

table_creation()