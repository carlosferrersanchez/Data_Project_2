import random
import numpy as np
import psycopg2
from faker import Faker
from ZZ_Auxiliar import car_types_list

class car:
    car_types_list = car_types_list
    used_driver_ids = []
    def __init__(self):
        self.driver_id = self.assign_random_id_driver()
        self.brand = random.choice(list(car.car_types_list.keys()))
        self.model = random.choice(list(car.car_types_list[self.brand].keys()))
        self.seats = car.car_types_list[self.brand][self.model]        
        adapted_to_dissability = ['Yes', 'No']
        probs_adapted_to_dissability = [0.20, 0.80]        
        self.dissability_readyness = np.random.choice(adapted_to_dissability,p=probs_adapted_to_dissability)  
    def assign_random_id_driver(self):
        try:
            conn = psycopg2.connect(
                dbname = "DP2",
                user = "postgres",
                password = "1234",
                host = "34.38.87.73", #Recuerda cambiar esto si lo dockerizas. Tendría que ser el nombre del contenedor "postgres"
                port = "5432"
            )
            cur = conn.cursor()
            cur.execute("SELECT id_driver FROM drivers WHERE id_driver NOT IN (SELECT id_driver FROM cars)")
            available_driver_ids = [row[0] for row in cur.fetchall()]

            if not available_driver_ids:
                raise Exception("No hay ID de conductor disponibles")

            driver_id = random.choice(available_driver_ids)
            return driver_id
        finally:
            cur.close()
            conn.close()

class driver:
    fake = Faker('es_ES')
    def __init__(self):
        self.name = driver.fake.first_name()
        self.surname = driver.fake.last_name()
        self.driver_license = driver.fake.unique.random_int(min=10000000, max=99999999)
        driver_status_types = ['Active','Inactive'] 
        probs_status = [0.0, 1]
        self.status = np.random.choice(driver_status_types,p=probs_status)  

class customer:
    fake = Faker('es_ES')
    def __init__(self):
        self.name = customer.fake.first_name()
        self.surname = customer.fake.last_name()
        self.email = customer.fake.email()


class BaseDeDatos:
    def __init__(self):
        self.conexion = psycopg2.connect(
            host='34.38.87.73',
            database='DP2',
            user='postgres',
            password='1234',
            port='5432'
        )
    def consultar(self, consulta_sql, parametros=None):
        with self.conexion.cursor() as cursor:
            cursor.execute(consulta_sql, parametros)
            return cursor.fetchall()

    def ejecutar(self, consulta_sql, parametros=None):
        with self.conexion.cursor() as cursor:
            cursor.execute(consulta_sql, parametros)
            self.conexion.commit()

    def cerrar(self):
        self.conexion.close()
