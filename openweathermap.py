#Installing necessary packages via cmd
#pip install openmeteo-requests
#pip install requests-cache retry-requests numpy pandas
#pip install click

# Importing packages
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


def weather(start, end,Lat=52.23,Lon=4.45):
    """
    The function takes in the location as defined by latitude and longitude (default values set to Nordwijk location),
    and arguments from the cli options, that are the start and end date defining the range 
    for which weather should be retrieved. It returns the temperature and humidity values.
    This function is modified from https://open-meteo.com/en/docs/historical-weather-api
    """

    # Setup the API client with cache and retry
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Inform user
    print(f"Retrieving weather data from {start} to {end} for Nordwijk (Lat: {Lat}, Lon: {Lon})...")

	# The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": Lat,
        "longitude": Lon,
        "start_date": start,
        "end_date": end,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "auto"
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    temperature = hourly.Variables(0).ValuesAsNumpy()
    humidity = hourly.Variables(1).ValuesAsNumpy()

    # Split the functions pd.to_datetime/timedelta and pd.date_range from the api for code clarity
    start_time = pd.to_datetime(hourly.Time(), unit="s", utc=True)
    end_time = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True)
    interval = pd.Timedelta(seconds=hourly.Interval())
    
    # Combine start end and frequency in one column
    time_range = pd.date_range(start=start_time, end=end_time, freq=interval, inclusive="left")

    # Creating pandas df with the extracted values
    df = pd.DataFrame({
        "datetime": time_range,
        "temperature_2m": temperature,
        "humidity_2m": humidity
    })

    # Print the success message + df preview 
    print("Weather data successfully retrieved:")
    print(df.head())

    # Save to CSV
    filename = f"weather_{start}_{end}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved data to {filename}")
    return df

