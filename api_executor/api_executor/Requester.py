import os
import requests
import json
from datetime import datetime, date

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class Requester():
    """
    This class is intended to be used to make requests to the API and save the response to a file.
    """

    def __init__(self, base_url: str, base_volume_path: str, endpoint: str):
        self.base_url = base_url
        self.base_volume_path = base_volume_path
        self.endpoint = endpoint
        self.clean_key = self.__extract_clean_key__(endpoint)
        self.dbutils = dbutils

    def get_request(self, endpoint: str = None, clean_key:str = None) -> json:
        """
        Performs the get request, if status == 200 then it will return
        the object as a json dict.
        Else, it will raise an Exception
        :param endpoint: (str) optional to override
        :param clean_key: (str) optional to override
        """

        # This code is used to override parameters
        if not endpoint:
            endpoint = self.endpoint
        if not clean_key:
            clean_key = self.clean_key

        url = self.base_url + endpoint
        print(f"Querying {url}")
        r = requests.get(url, headers={"accept": "application/json"})
        if r.status_code == 200:
            r_json = self.__extract_value_from_response__(r, clean_key)
            if len(r_json) > 0: 
                return r_json
        else:
            raise Exception(f"Error: {r.status_code} - {r.text}")
    
    def save_file(self, response: dict) -> None:
        """
        Saves the file into the base_path location by using the current timestamp.
        :param response: dict
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.clean_key}_{timestamp}.json"
        full_path = os.path.join(self.base_volume_path, self.clean_key)
        self.dbutils.fs.mkdirs(full_path)
        full_filename = os.path.join(full_path, filename)
        
        with open(full_filename, "w") as f:
            try:
                json.dump(response, f)
                print("File written")
            except Exception:
                print(f"Error writing {full_filename}")

    
    def independent_api(self) -> None:
        """
        Manage queries when API has no dependencies.
        """
        print(f"clean_key: {self.clean_key}")
        response = (self.get_request())
        self.save_file(response)


    def dependent_api(self, dependencies: list) -> None:
        """
        Manage queries when API has dependencies
        """
        print(f"clean_key: {self.clean_key}")
        if self.clean_key == "sensors":
            response = self.get_request(dependencies[0].get("endpoint", ""), "stations")

            weather_stations = [res.get("id_weatherstation", "") for res in response]
            response = []
            for weather_station in weather_stations:
                temp_response = (
                    self.get_request(
                        self.endpoint.replace("{idWeatherstation}", str(weather_station)), "stations"
                    )
                )
                sensors = temp_response[0].get("sensors",[])
                if len(sensors) > 0:
                    for sensor in sensors:
                        response.append(sensor)
                        
        elif self.clean_key == "readings":
            response = self.get_request(dependencies[0].get("endpoint", ""), "stations")
            weather_stations = [res.get("id_weatherstation", "") for res in response]
            response = []
            year = int(date.today().year)
            month = int(date.today().month)
            if month == 1:
                year = year - 1
                month = 12
            else:
                month = month - 1
            for weather_station in weather_stations:
                query_param = str(year) + "?month=" + str(month)
                temp_response = (
                    self.get_request(
                        self.endpoint.replace("{idWeatherstation}", str(weather_station)).replace("{year}", query_param), "summarized"
                    )
                )
                response.append(temp_response)
        
        self.save_file(response)       

    @staticmethod
    def __extract_clean_key__(endpoint: str) -> str:
        """
        Cleans the endpoint to create the key that is going
        to be used for multiple steps.
        :param endpoint: str
        :return: str
        """
        potential = str(endpoint).split("/")[-1]
        if "{" in potential:
            return str(endpoint).split("/")[1]
        return potential
    
    @staticmethod
    def __extract_value_from_response__(response: json, endpoint_key: str) -> list:
        """
        Extracts the falue from the response and returns a list
        depending on the result, it can be an empty list.
        :param response: (json)
        :param endpoint_key: str
        :return: list  
        """
        r_json = response.json()
        r_json = r_json.get(endpoint_key, [])
        return r_json

def runner(operation):
    # Get parameters
    parameters = json.loads(operation.get("parameters", {}))
    base_url = parameters.get("base_url")
    base_volume_path = parameters.get("base_volume_path")
    endpoint = parameters.get("endpoint")
    dependencies = parameters.get("dependencies", [])

    # Create initial base
    r = Requester(
        base_url = base_url,
        base_volume_path = base_volume_path,
        endpoint= endpoint)
    if len(dependencies) > 0:
        r.dependent_api(dependencies)
    else:
        r.independent_api()
    