from FlightRadar24.api import FlightRadar24API
from pyspark.sql.types import *
from pyspark.sql.functions import *

def getDataFlightRadarAPI():
    return FlightRadar24API()
     
def extract_airlines_toDF(airlines, spark):
    def convert_airlines(lst):
        for airline in lst:
            airline['Code'] = str(airline['Code'])
            airline['ICAO'] = str(airline['ICAO'])
            airline['Name'] = str(airline['Name'])
        return lst 

    schema_airlines = StructType([
        StructField("Code", StringType(), True),
        StructField("ICAO", StringType(), True),
        StructField("Name", StringType(), True)
    ])
    #airlines = fr_api.get_airlines()
    airlines = convert_airlines(airlines)
    
    df_airlines = spark.createDataFrame(airlines, schema=schema_airlines)
    return df_airlines, spark


def extract_airports_toDF(airports, spark):
    """def convert_airports(lst):
        for airport in lst:
            airport['name'] = str(airport['name'])
            airport['iata'] = str(airport['iata'])
            airport['icao'] = str(airport['icao'])
            airport['country'] = str(airport['country'])
            airport['lat'] = float(airport['lat'])
            airport['lon'] = float(airport['lon'])
            airport['alt'] = int(airport['alt'])
        return lst
        """
    def convert_airports(lst):
        df = []
        for idx, airport in enumerate(lst):
            if idx !=0:
                el = {}
                el['name'] = str(airport['name'])
                el['iata'] = str(airport['iata'])
                el['icao'] = str(airport['icao'])
                el['country'] = str(airport['country'])
                el['lat'] = float(airport['lat'])
                el['lon'] = float(airport['lon'])
                el['alt'] = int(airport['alt'])

                df.push(el)
        return df
        
    #airports = fr_api.get_airports()
    airports_typed_columns = convert_airports(airports)

    schema_airports = StructType([
    StructField("name", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("alt", IntegerType(), True)
    ])
    df_airports = spark.createDataFrame(airports_typed_columns, schema=schema_airports)

    return df_airports, spark


def extract_flights_toDF(fr_api, flights, spark):
   def convert_flights(lst):
      for flight in lst:
         flight = flight.__dict__
         flight['id'] = str(flight['id'])
         flight['icao_24bit'] = str(flight['icao_24bit'])
         flight['latitude'] = float(flight['latitude'])
         flight['longitude'] = float(flight['longitude'])
         flight['heading'] = int(flight['heading'])
         flight['altitude'] = int(flight['altitude'])
         flight['ground_speed'] = int(flight['ground_speed'])
         flight['squawk'] = str(flight['squawk'])
         flight['aircraft_code'] = str(flight['aircraft_code'])
         flight['registration'] = str(flight['registration'])
         flight['time'] = int(flight['time'])
         flight['origin_airport_iata'] = str(flight['origin_airport_iata'])
         flight['destination_airport_iata'] = str(flight['destination_airport_iata'])
         flight['number'] = str(flight['number'])
         flight['airline_iata'] = str(flight['airline_iata'])
         flight['on_ground'] = int(flight['on_ground'])
         flight['vertical_speed'] = int(flight['vertical_speed'])
         flight['callsign'] = str(flight['callsign'])
         flight['airline_icao'] = str(flight['airline_icao'])
         
         details = fr_api.get_flight_details(flight['id'])
         """print(details)
         print("_________________")
         print(details['time']['real']['departure'])
         print("_________________")
         print(isinstance(details['time']['real']['departure'], type(None)))"""
         #if type(details) is dict and isinstance(details['time']['real']['departure'], type(None)) != True \
            #and isinstance(details['time']['real']['arrival'], type(None)) != True :

         if type(details) is dict and 'time' in details and 'real'in details['time'] and isinstance(details['time']['real']['departure'], type(None)) != True \
            and isinstance(details['time']['real']['arrival'], type(None)) != True :
               flight['time_depart'] = int(details['time']['real']['departure'])
               flight['time_arrive'] = int(details['time']['real']['arrival'])

      return lst

   #flights = fr_api.get_flights()
   flights = convert_flights(flights)
   
   schema = StructType([
   StructField("id", StringType(), True),
   StructField("icao_24bit", StringType(), True),
   StructField("latitude", FloatType(), True),
   StructField("longitude", FloatType(), True),
   StructField("heading", IntegerType(), True),
   StructField("altitude", IntegerType(), True),
   StructField("ground_speed", IntegerType(), True),
   StructField("squawk", StringType(), True),
   StructField("aircraft_code", StringType(), True),
   StructField("registration", StringType(), True),
   StructField("time", IntegerType(), True),
   StructField("origin_airport_iata", StringType(), True),
   StructField("destination_airport_iata", StringType(), True),
   StructField("number", StringType(), True),
   StructField("airline_iata", StringType(), True),
   StructField("on_ground", IntegerType(), True),
   StructField("vertical_speed", IntegerType(), True),
   StructField("callsign", StringType(), True),
   StructField("airline_icao", StringType(), True),
   StructField("time_depart", IntegerType(), True),
   StructField("time_arrive", IntegerType(), True)
   ])

   df_flights = spark.createDataFrame(flights, schema=schema)
   return df_flights, spark


def extract_zones_toDF(fr_api, spark):
    def process_zones(tmp, result, root):
        for el in tmp:
            result.append([str(el), str(root), float(tmp[el]['tl_y']), float(tmp[el]['tl_x']), float(tmp[el]['br_y']), float(tmp[el]['br_x'])])
            
            if 'subzones' in tmp[el]:
                process_zones(tmp[el]['subzones'], result, el)
                
    array_zones = []
    zones = fr_api.get_zones()
    process_zones(zones,array_zones, None)

    schema_zones = StructType([
    StructField("Subzone", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("br_x", FloatType(), True),
    StructField("br_y", FloatType(), True),
    StructField("tl_x", FloatType(), True),
    StructField("tl_y", FloatType(), True)
    ])
    
    df_zones = spark.createDataFrame(array_zones, schema=schema_zones)
    df_zones = df_zones.withColumn("Subzone", initcap(col('Subzone')))
    df_zones = df_zones.withColumn("Zone", initcap(col('Zone')))

    return df_zones, spark