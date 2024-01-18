from flask import Flask, jsonify
import psycopg2
from datetime import datetime
# Para acceder desde localhost: http://localhost:5000/new_active_vehicle
# Para acceder desde dentro de docker: http://flask:5000/new_active_vehicle


app = Flask(__name__)

def get_inactive_car_and_driver():
    conn = psycopg2.connect(
        dbname="DB_DP2",
        user="dp2",
        password="dp2",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT id_driver
        FROM drivers
        WHERE driver_status = 'Inactive'
        AND id_driver NOT IN (
            SELECT id_driver
            FROM active_vehicles
        )                
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

    cur.close()
    conn.close()

    return id_driver, id_route

@app.route('/new_active_vehicle', methods=['GET'])
def activate_vehicle():
    id_driver, id_route = get_inactive_car_and_driver()
    return jsonify({
        'id_driver': id_driver[0] if id_driver else None,
        'id_route': id_route[0] if id_route else None
    })

def activate_API_active_vehicles():
    app.run(host='0.0.0.0', port=5000)

activate_API_active_vehicles()
