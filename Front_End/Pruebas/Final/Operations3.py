import streamlit as st
import pandas as pd
import psycopg2
import time
from getpass import getpass

# Class to handle the database
class Database:
    # Database credentials as static attributes
    HOST = '34.38.87.73'
    DATABASE = 'DP2'
    USER = 'postgres'
    PASSWORD = getpass("Introduce la contraseña de la base de datos: ")
    PORT = '5432'

    @staticmethod
    def connect():
        return psycopg2.connect(
            host=Database.HOST,
            database=Database.DATABASE,
            user=Database.USER,
            password=Database.PASSWORD
        )

    @staticmethod
    def query(query_sql, params=None):
        with Database.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query_sql, params)
                return cursor.fetchall()

    @staticmethod
    def execute(query_sql, params=None):
        with Database.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query_sql, params)
                connection.commit()

# Adjust margins
custom_css = """
<style>
body {
    margin: 0px;
}

[data-testid="stTheme"] {
    background-color: #f9f2f2 !important;
    font-family: serif !important;
}

[data-testid="stSidebar"] {
    background-color: #04b194 !important;
}

[data-testid="stBlockContainer"] {
    background-color: #e6e6e6 !important;
}
</style>
"""

blue = "<style>div.stDivider.horizontal {border: 2px solid #04b194;}</style>"

# Logo in the sidebar
st.sidebar.image('/Users/paulglobal/Documents/GitHub/Data_Project_2/Front_End/Pruebas/Final/LOGO.png', use_column_width=True)

# The rest of your Streamlit application
st.title('City Operations Dashboard')
st.markdown(blue, unsafe_allow_html=True)  # Blue divider
st.header('_OnlyRides_ is here: :blue[Valencia] :sunglasses:')


# Function to count the number of vehicles with service_status "With Passengers"
def count_vehicles():
    query_with_passengers = "SELECT COUNT(*) FROM active_vehicles WHERE service_status = 'With Passengers'"

    try:
        with Database.connect() as connection:
            count_with_passengers = Database.query(query_with_passengers)[0][0]
            return count_with_passengers
    except Exception as e:
        st.error(f'Error counting vehicles: {e}')
        return None

# Get the count of vehicles with service_status "With Passengers"
count_with_passengers = count_vehicles()

# Display the number of vehicles with passengers
if count_with_passengers is not None:
    st.success(f'Vehicles with passengers on board: {count_with_passengers}')
else:
    st.warning("Unable to retrieve vehicle information. Please try again later.")

# Configure auto-refresh in the sidebar
st.sidebar.title("Auto Refresh")
auto_refresh = st.sidebar.checkbox('Select Time', False)
if auto_refresh:
    refresh_rate = st.sidebar.number_input('Refresh rate in seconds', min_value=1, value=10)

# Function to load and display data
def load_data_and_display(active=True):
    while True:
        # Display metrics
        cols = st.columns(3)
        with cols[0]:
            query_active_customers = "SELECT COUNT(*) FROM customers WHERE customer_status = 'Active'"
            total_active_customers = Database.query(query_active_customers)[0][0]
            if total_active_customers is not None:
                st.metric("Active Customers", total_active_customers, "Customers using the app")

        with cols[1]:
            query_avg_rating = "SELECT AVG(rating) FROM rating"
            avg_rating = Database.query(query_avg_rating)[0][0]

            if avg_rating is not None:
                st.metric("Average Rating", f"{avg_rating:.2f}", "Customer satisfaction")
        
        with cols[2]:
            query_active_drivers = "SELECT COUNT(*) FROM drivers WHERE driver_status = 'Active'"
            total_active_drivers = Database.query(query_active_drivers)[0][0]
            if total_active_drivers is not None:
                st.metric("Active Drivers", total_active_drivers, "Drivers on active routes")

        # Toggle switch to alternate between active rides and all rides
        show_active_rides = st.checkbox('View all active vehicles', True)
        
        # Map explanation
        st.caption('Map of vehicles with passengers on board')
        
        # Function to load ride data
        def load_ride_data(active=True):
            if active:
                query_sql = "SELECT id_driver, current_position FROM active_vehicles WHERE service_status IN ('With Passengers')"
            else:
                query_sql = "SELECT ride_current_position FROM rides"

            try:
                data = Database.query(query_sql)
                if active:
                    df = pd.DataFrame(data, columns=['id_driver', 'position'])
                    df['lat'] = df['position'].apply(lambda x: float(x.split(',')[1].strip(')')))
                    df['lon'] = df['position'].apply(lambda x: float(x.split(',')[0].strip('POINT(')))
                    return df[['id_driver', 'lat', 'lon']]
                else:
                    df = pd.DataFrame(data, columns=['position'])
                    df['lat'] = df['position'].apply(lambda x: float(x.split(',')[1].strip(')')))
                    df['lon'] = df['position'].apply(lambda x: float(x.split(',')[0].strip('POINT(')))
                    return df[['lat', 'lon']]
            except Exception as e:
                st.error(f'Error loading ride data: {e}')
                return pd.DataFrame(columns=['id_driver', 'lat', 'lon'])

        # Query SQL to get customers waiting for their ride
        query_sql_customers = """
        SELECT rr.id_request, c.name, c.surname
        FROM ride_requests rr
        JOIN customers c ON rr.id_customer = c.id_customer
        WHERE rr.request_status = 'Active' AND c.customer_status = 'Active'
        """

        # Execute the SQL query and get the data into a DataFrame
        with Database.connect() as connection:
            df_customers = pd.read_sql_query(query_sql_customers, connection)

        # Query SQL to get active drivers
        query_sql_active_drivers = """
        SELECT id_driver, name, surname, driver_license
        FROM drivers
        WHERE driver_status = 'Active'
        """

        # Execute the SQL query and get the data into a DataFrame
        with Database.connect() as connection:
            df_active_drivers = pd.read_sql_query(query_sql_active_drivers, connection)

        # Load and display the map with the specified rides
        ride_data = load_ride_data(show_active_rides)
        if not ride_data.empty:
            st.map(ride_data)
        else:
            st.write("No rides to display.")

        # Display the queue of customers waiting for their ride
        st.header('Customers in queue waiting for their ride:')
        if not df_customers.empty:
            st.table(df_customers)
        else:
            st.write("No customers in queue at the moment.")

        # Display active drivers
        st.header('Active Drivers:')
        if not df_active_drivers.empty:
            st.table(df_active_drivers)
        else:
            st.write("No active drivers at the moment.")

        # Wait for the specified time in seconds
        if auto_refresh:
            time.sleep(refresh_rate)
            st.experimental_rerun()

# Call the function to load and display the data
load_data_and_display(True)
