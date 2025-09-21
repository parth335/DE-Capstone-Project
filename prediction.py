import pandas as pd
import psycopg2
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np

# --- Configuration (REUSE CREDENTIALS) ---
DB_HOST = "localhost"
DB_NAME = "weather_db"
DB_USER = "postgres"
DB_PASSWORD = "root"
CITY_NAME_TO_ANALYZE = "New York" 
# ------------------------------------------

def fetch_data_for_ml():
    """Fetches the last 30 days of data and returns a DataFrame."""
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        query = f"""
        SELECT timestamp, temperature
        FROM historical_weather
        WHERE city = '{CITY_NAME_TO_ANALYZE}'
          AND timestamp >= NOW() - INTERVAL '30 days'
        ORDER BY timestamp;
        """
        df = pd.read_sql(query, conn)
        return df
    except (Exception, psycopg2.Error) as error:
        print(f"Error fetching ML data: {error}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def predict_weather_with_ml():
    df = fetch_data_for_ml()
    
    if df.empty:
        print("Not enough data to train the model.")
        return

    # 1. Feature Engineering: Create a numerical time feature (days elapsed)
    df.set_index('timestamp', inplace=True)
    df['day_index'] = (df.index - df.index.min()).total_seconds() / (3600 * 24) # Time in days
    
    # Define features (X) and target (y)
    X = df[['day_index']].values.reshape(-1, 1) 
    y = df['temperature'].values.reshape(-1, 1)

    # 2. Train-Test Split (80% Train, 20% Test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    # 3. Model Training: Simple Linear Regression
    model = LinearRegression()
    model.fit(X_train, y_train)

    # 4. Evaluation
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f"\n--- Model Evaluation (Linear Regression) ---")
    print(f"Mean Squared Error (MSE): {mse:.2f}")

    # 5. Forecasting: Predict the temperature for the next day
    
    # Index for the point exactly 1 day after the last recorded data point
    last_day_index = df['day_index'].max()
    future_day_index = np.array([[last_day_index + 1]])
    
    predicted_temp = model.predict(future_day_index)[0][0]

    print(f"\n--- Basic ML Forecast ---")
    print(f"Predicted Temperature for 1 day ahead in {CITY_NAME_TO_ANALYZE}: **{predicted_temp:.2f}°C**")

if __name__ == '__main__':
    predict_weather_with_ml()