import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from datetime import timedelta

# --- Configuration (REUSE CREDENTIALS) ---
DB_HOST = "localhost"
DB_NAME = "weather_db"
DB_USER = "postgres"
DB_PASSWORD = "root"
CITY_NAME_TO_ANALYZE = "New York" 
# ------------------------------------------

def visualize_weather_data():
    """Queries the last 30 days of data and creates a plot."""
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        # SQL query to fetch data for the last 30 days for a specific city
        query = f"""
        SELECT timestamp, temperature, humidity, pressure
        FROM historical_weather
        WHERE city = '{CITY_NAME_TO_ANALYZE}'
          AND timestamp >= NOW() - INTERVAL '30 days'
        ORDER BY timestamp;
        """
        
        # Use pandas to read data directly from the SQL query
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("No data found for the last 30 days. Run the data pipeline first.")
            return

        df.set_index('timestamp', inplace=True)

        # --- Plotting ---
        fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(12, 10), sharex=True)
        fig.suptitle(f'30-Day Weather Analysis for {CITY_NAME_TO_ANALYZE}', fontsize=16, weight='bold')

        # Temperature Plot
        axes[0].plot(df['temperature'], label='Temperature (°C)', color='red', linewidth=1.5)
        axes[0].set_ylabel('Temperature (°C)')
        axes[0].set_title('Temperature over Time')
        axes[0].grid(True, linestyle='--', alpha=0.6)

        # Humidity Plot
        axes[1].plot(df['humidity'], label='Humidity (%)', color='blue', linewidth=1.5)
        axes[1].set_ylabel('Humidity (%)')
        axes[1].set_title('Humidity over Time')
        axes[1].grid(True, linestyle='--', alpha=0.6)

        # Pressure Plot
        axes[2].plot(df['pressure'], label='Pressure (hPa)', color='green', linewidth=1.5)
        axes[2].set_ylabel('Pressure (hPa)')
        axes[2].set_xlabel('Date/Time')
        axes[2].set_title('Pressure over Time')
        axes[2].grid(True, linestyle='--', alpha=0.6)

        plt.xticks(rotation=45)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.show()

    except (Exception, psycopg2.Error) as error:
        print(f"Error during visualization: {error}")
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    visualize_weather_data()