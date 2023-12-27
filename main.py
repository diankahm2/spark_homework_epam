from os import listdir
from os.path import isfile, join

import geohash2
from opencage.geocoder import OpenCageGeocode
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@F.udf()
def getCoords(address):
    """
    Function to get lat and lng from OpenCageGeocode API
    :param address: address
    :return: latitude and longitude
    """
    key = ""
    geocoder = OpenCageGeocode(key)

    results = geocoder.geocode(address)

    if len(results) > 0:
        lat = results[0]["geometry"]["lat"]
        long = results[0]["geometry"]["lng"]
    else:
        return None

    return lat, long


@F.udf(StringType())
def generate_geohash(lat, lang):
    """
    Udf function to get geohash using geohash2 library
    :param lat: latitude
    :param lang: longitude
    :return: encoded geohash
    """
    geohash = geohash2.encode(float(lat), float(lang), precision=4)
    return geohash


def getRestaurants():
    """
    Get all csv files of restaurants in directory
    :return: list of restaurants csv
    """
    restaurants_files = [
        join(rest_path, f) for f in listdir(rest_path) if isfile(join(rest_path, f))
    ]
    return restaurants_files


def restDf(rest_list):
    """
    Function which prepare restaurant dataframe
    1. Searching null values
    2. Mapping null values address with API
    4. Split into lat, long
    5. Union into one dataframe
    6. In case even API can not find necessary values, drop them
    7. Add geohash

    :param rest_list:
    :return: clean restaurants df with hash
    """
    restaurants = spark.read.csv(rest_list, header=True)

    null_coords = restaurants.filter((F.col("lat").isNull()) | (F.col("lng").isNull()))
    null_coords = null_coords.withColumn(
        "address",
        F.concat_ws(" ", F.col("country"), F.col("city"), F.col("franchise_name")),
    ).drop("lat", "lng")

    filled_coords = null_coords.withColumn("coord", getCoords(null_coords.address))
    filled_coords = (
        filled_coords.withColumn("lat", F.split("coord", ",")[0].cast("double"))
        .withColumn("long", F.split("coord", ",")[1].cast("double"))
        .drop("coord", "address")
    )

    full_restaurants = restaurants.union(filled_coords).na.drop()

    restaurants = full_restaurants.withColumn(
        "geohash", generate_geohash(restaurants.lat, restaurants.lng)
    )

    return restaurants


def weatherDf():
    """
    Prepare weather datafame
    :return: weather datafame
    """
    weather = spark.read.parquet(weather_path)
    weather = (
        weather.withColumn("geohash", generate_geohash(weather.lat, weather.lng))
        .drop("lat", "lng")
    )
    return weather


def mainDf(restaurants, weather):
    """
    Join weather and restaurants dataframe and write by partion
    :param restaurants:
    :param weather:
    :return: 0
    """
    df = restaurants.join(weather, on="geohash", how="left")
    df.write.partitionBy("wthr_date").parquet(final_path)

    return df


def testCase(df):
    """
    Test case to check df empty or contains null values
    :param df:
    :return:
    """
    assert df.count() == 0
    for column in df.columns:
        null_count = df.filter(F.col(column).isNull()).count()
        assert null_count == 0, "DF null columns" 
    
    
def main():
    """
    Main function, execute all logic
    :return: o
    """
    rest_list = getRestaurants()
    restaurants = restDf(rest_list)
    weather = weatherDf()
    df = mainDf(restaurants, weather)
    testCase(df)

    return 0


if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_practice").getOrCreate()
    rest_path = (
        "restaurant_csv"
    )
    weather_path = "weather"
    final_path = "main"

    main()
