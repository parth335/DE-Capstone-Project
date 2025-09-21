CREATE TABLE historical_weather (
    record_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature NUMERIC(5, 2), 
    humidity INTEGER,         
    pressure INTEGER,          
    description VARCHAR(255)
);
SELECT * FROM historical_weather;
-- Example to push the first 5 records back in time
UPDATE historical_weather
SET timestamp = NOW() - INTERVAL '25 days'
WHERE record_id = 1;

UPDATE historical_weather
SET timestamp = NOW() - INTERVAL '15 days'
WHERE record_id = 2;
SELECT * FROM historical_weather;

SELECT * FROM historical_weather;
SELECT * FROM historical_weather;

