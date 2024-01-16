from flask import Flask, jsonify, request, abort
import psycopg2
from datetime import datetime
import random

conn = psycopg2.connect(
        dbname="DB_DP2",
        user="dp2",
        password="dp2",
        host="localhost",
        port="5432"
    )
cur = conn.cursor()

def get_inactive_car_and_driver():
    cur.execute("""
        SELECT id_car
        FROM cars
        WHERE car_status = 'Inactive'
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_car = cur.fetchone()

    cur.execute("""
        SELECT id_driver
        FROM drivers
        WHERE driver_status = 'Inactive'
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_driver = cur.fetchone()

    cur.execute("""
        SELECT id_route
        FROM routes
        ORDER BY RANDOM()
        LIMIT 1
    """)
    id_route = cur.fetchone()

    return id_car, id_driver, id_route

app = Flask(__name__)

@app.route('/new_active_vehicle', methods=['GET'])

def activate_vehicle():
    id_car, id_driver, id_route = get_inactive_car_and_driver()
    return jsonify({
        'id_car': id_car[0] if id_car else None,
        'id_driver': id_driver[0] if id_driver else None,
        'id_route': id_route[0] if id_route else None
    })

if __name__ == '__main__':
    app.run(debug=True)