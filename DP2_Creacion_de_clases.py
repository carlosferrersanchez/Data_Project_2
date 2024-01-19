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
                dbname="DB_DP2",
                user="dp2",
                password="dp2",
                host="localhost",
                port="5432"
            )
            cur = conn.cursor()
            cur.execute("SELECT id_driver FROM drivers")
            available_driver_ids = [row[0] for row in cur.fetchall()]
            unused_driver_ids = [driver_id for driver_id in available_driver_ids if driver_id not in car.used_driver_ids]
            if unused_driver_ids:
                driver_id = random.choice(unused_driver_ids)
                car.used_driver_ids.append(driver_id)
                return driver_id
        finally:
            cur.close()
            conn.close()

'''       
#PRUEBA DE QUE FUNCIONA:
prueba_coches = [car() for _ in range(10)]
for coche in prueba_coches:
    print(f"Brand: {coche.car_brand}, Model: {coche.car_model}, Car Seats: {coche.car_seats}, Status: {coche.car_status}, Adapted to dissability {coche.dissability_readyness}")
'''

class driver:
    fake = Faker('es_ES')
    def __init__(self):
        self.name = driver.fake.first_name()
        self.surname = driver.fake.last_name()
        self.driver_license = driver.fake.unique.random_int(min=10000000, max=99999999)
        driver_status_types = ['Active','Inactive'] 
        probs_status = [0.0, 1]
        self.status = np.random.choice(driver_status_types,p=probs_status)  
'''     
#PRUEBA DE QUE FUNCIONA:
prueba_conductores = [driver() for _ in range(10)]
for conductor in prueba_conductores:
    print(f"Nombre: {conductor.name}, Apellido: {conductor.surname}, Driver License: {conductor.driver_license}")
'''
class customer:
    fake = Faker('es_ES')
    def __init__(self):
        self.name = customer.fake.first_name()
        self.surname = customer.fake.last_name()
        self.email = customer.fake.email()

'''       
#PRUEBA DE QUE FUNCIONA:
prueba_clientes = [customer() for _ in range(10)]
for cliente in prueba_clientes:
    print(f"Nombre: {cliente.customer_name}, Apellido: {cliente.customer_surname}, Email: {cliente.customer_email}")
'''