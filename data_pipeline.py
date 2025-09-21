import requests
import psycopg2
from datetime import datetime

# --- Configuration (UPDATE THIS SECTION) ---
API_KEY = "7f3cbf441502efb8b227e8768d1c4e84"

CITY_NAME = "New York"
LAT = 40.7128
LON = -74.0060
# OpenWeatherMap current weather API endpoint (using metric units)
WEATHER_API_URL = f"http://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric" 

# Database Credentials
DB_HOST = "localhost"
DB_NAME = "weather_db"
DB_USER = "postgres"
DB_PASSWORD = "root"
# ------------------------------------------

def fetch_weather_data():
    """Fetches current weather data from the API."""
    try:
        response = requests.get(WEATHER_API_URL)
        response.raise_for_status() # Check for bad status codes
        data = response.json()
        
        # Data Extraction
        record = {
            'city': CITY_NAME,
            'timestamp': datetime.now(),
            'temp': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'description': data['weather'][0]['description']
        }
        return record
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def store_weather_data(record):
    """Stores a single weather record into PostgreSQL."""
    if not record: return

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO historical_weather (city, timestamp, temperature, humidity, pressure, description)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        
        cur.execute(insert_query, (
            record['city'], record['timestamp'], record['temp'], 
            record['humidity'], record['pressure'], record['description']
        ))
        
        conn.commit()
        print(f"Data stored successfully at {record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        
    except (Exception, psycopg2.Error) as error:
        print(f"DB Error: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    data = fetch_weather_data()
    store_weather_data(data)