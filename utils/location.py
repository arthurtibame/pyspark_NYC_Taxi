from pyspark.sql.types import *
import pyspark.sql.functions as F

class AddressProcess(object):
    """[summary]
        customized Address UDF Object
    
    [CAUTION]
    "" return value may be CN or EN""
    ######################################
    [usage]
        inputs:
            Latitude -> float
            Longtitude -> float
        funcs:
            house_numberUDF
            roadUDF
            suburbUDF
            townUDF
            countyUDF
            stateUDF
            postcodeUDF
            countryUDF
            country_codeUDF
    """
    from geopy.geocoders import Nominatim        

    @staticmethod
    @F.udf(returnType=StringType())
    def house_numberUDF(Lat, Lon):
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["house_number"]        
        except:
            return None

    @staticmethod
    @F.udf(returnType=StringType())
    def roadUDF(Lat, Lon):    
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["road"]
        except:
            return None
    
    @staticmethod
    @F.udf(returnType=StringType())
    def suburbUDF(Lat, Lon):    
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["suburb"]
        except:
            None

    @staticmethod
    @F.udf(returnType=StringType())
    def townUDF(Lat, Lon):    
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["town"]
        except:
            return None

    @staticmethod
    @F.udf(returnType=StringType())
    def countyUDF(Lat, Lon):    
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)              
        try:
            return location.raw["address"]["county"]
        except:
            return None

    @staticmethod
    @F.udf(returnType=StringType())
    def stateUDF(Lat, Lon):    
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["state"]
        except:
            return None
    @staticmethod
    @F.udf(returnType=StringType())
    def postcodeUDF(Lat, Lon):    
        # town, county, state, postcode
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["postcode"]
        except:
            return None            

    @staticmethod
    @F.udf(returnType=StringType())
    def countryUDF(Lat, Lon):    
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["country"]
        except:
            return None        

    @staticmethod
    @F.udf(returnType=StringType())
    def country_codeUDF(Lat, Lon):          
        # town, county, state, postcode
        location = f"{str(Lat)},{str(Lon)}"
        geolocator = Nominatim(user_agent="http")
        location = geolocator.reverse(location)
        try:
            return location.raw["address"]["country_code"]                        
        except:
            return None        

