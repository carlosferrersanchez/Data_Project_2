import random
import numpy as np
from faker import Faker
from ZZ_Auxiliar import car_types_list

class car:
    car_types_list = car_types_list
    def __init__(self):
        self.brand = random.choice(list(car.car_types_list.keys()))
        self.model = random.choice(list(car.car_types_list[self.brand].keys()))
        self.seats = car.car_types_list[self.brand][self.model]
 
        car_status_types = ['Active','Inactive','Maintenance'] 
        probs_status = [0.0, 0.90, 0.10]
        self.status = np.random.choice(car_status_types,p=probs_status)           
        
        adapted_to_dissability = ['Yes', 'No']
        probs_adapted_to_dissability = [0.20, 0.80]        
        self.dissability_readyness = np.random.choice(adapted_to_dissability,p=probs_adapted_to_dissability)  

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