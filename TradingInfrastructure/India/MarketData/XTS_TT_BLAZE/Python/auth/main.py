import requests as rqs
import configparser
import os
import sys
import json

'''
These are some parameters to pass to use market_data_api class: 
url: The base URL for the API. 
access_password: Password for accessing the host lookup. 
version: Version of the host lookup API. 
secretKey: Secret key for market API authentication. 
apiKey: API key for market API authentication. 
'''


from logger.main import LoggerBase


class MarketDataApiCredentials(LoggerBase):
    def __init__(self, 
                 url = "https://ttblaze.iifl.com", 
                 access_password = "2021HostLookUpAccess",
                 version = "interactive_1.0.1",
                 secretKey = None,
                 apiKey = None
                 ):

        super().__init__()

        self.url = url
        self.access_password = access_password
        self.version = version
        self.apiKey = apiKey
        self.secretKey = secretKey
        self.auth_token = None
        self.token = None

        self.config_file_path = './login.ini'
        self.config = configparser.ConfigParser()

          
    async def save_to_config(self):
        self.info("Attempting to save credentials to config file: %s", self.config_file_path)

        try:
            with open(self.config_file_path, 'w') as configfile:
                self.config.write(configfile)
            self.info("Credentials successfully saved to config file: %s", self.config_file_path)
        except IOError as io_err:
            self.ioError("Error writing to config file: %s", io_err)
            raise 


    async def HostLookUp(self):
        
        if not self.config_file_path:
            self.FileError("Cannot Have `config_file_path` empty")
            raise 
        
        self.config.read(str(self.config_file_path))
        self.secretKey = self.config['AUTH']['secret_key'] or None
        self.apiKey = self.config['AUTH']['api_key'] or None

        if not self.auth_token:
            self.error(fr"`auth_token` is empty inside {self.__class__.__name__}.HostLookUp")
            raise ValueError(fr"`auth_token` is empty inside {self.__class__.__name__}.HostLookUp")
    
        HOST_LOOKUP_URL = fr"{self.url}:4000/HostLookUp"
        self.config.read(self.config_file_path)
        payload_host_lookup = {
            "accesspassword": self.access_password,
            "version": self.version
        }

        if self.auth_token is None:
            self.config.read(self.config_file_path)
            self.auth_token = self.config['AUTH'].get('unique_key') or None
        
        if self.auth_token is None:
            try:
                response = rqs.post(url = HOST_LOOKUP_URL, json = payload_host_lookup)
            except rqs.exceptions.HTTPError as http_err:
                self.error(f"HTTP error occurred during host lookup: {http_err}")
                return None
            except rqs.exceptions.ConnectionError as conn_err:
                self.error(f"Connection error occurred during host lookup: {conn_err}")
                return None
            except rqs.exceptions.Timeout as timeout_err:
                self.error(f"Timeout error occurred during host lookup: {timeout_err}")
                return None
            except rqs.exceptions.RequestException as req_err:
                self.error(f"An error occurred during host lookup: {req_err}")
                return None

            try:
                self.info(fr"Reponse From HostLookUp | {response} : {response.json()}")
                response_data = response.json()
            except ValueError as json_err:
                self.error(f"JSON decode error: {json_err}")
                return None

            unique_key = response_data["result"]["uniqueKey"]
            
            if unique_key is None:
                await self.error("uniqueKey generated is empty")
                raise ValueError("uniqueKey generated is empty")

            self.auth_token = unique_key
            self.config["AUTH"]["unique_key"] = unique_key

            try:
                with open(self.config_file_path, 'w') as configfile:
                    self.config.write(configfile)
                print("HostLookUp was successful")
            except IOError as io_err:
                print(f"Error writing to config file: {io_err}")
                return None
        
        return self.auth_token  
        

    async def login(self):
        self.config.read(str(self.config_file_path))
        self.auth_token = self.config['AUTH']['unique_key'] or None
        self.secretKey = self.config['AUTH']['secret_key'] or None
        self.apiKey = self.config['AUTH']['api_key'] or None

        if self.auth_token is None:
            self.auth_token = self.HostLookUp()

        if not all([self.secretKey,self.apiKey]):
            self.warning("Missing Login Parameters")
            raise ValueError("Missing Login Parameters")

        payload_market_data = {
            "secretKey": fr"{self.secretKey}",
            "appKey": fr"{self.apiKey}",
            "source": "WEB"
        }
        
        login_header_market_data = {
            "Content-Type": "application/json",
            "authorization": fr"{self.auth_token}"
        }
        
        LOGIN_URL_MARKET_API = fr"{self.url}/apimarketdata/auth/login"
        response_market_data_login = rqs.post(url = LOGIN_URL_MARKET_API, 
                                              headers = login_header_market_data, 
                                              json = payload_market_data)
        
        if response_market_data_login.status_code == 200:
            login_response = response_market_data_login.json()
            self.info(fr"Login Response: {login_response}")
            self.token = login_response.get('result').get('token')
            self.config['AUTH']['token'] = self.token
            with open(str(self.config_file_path),'w') as configfile:
                self.config.write(configfile)
            print(fr"Login Was Successful | {self.token}")
            self.info(fr"Login Was Successful | {self.token}")
            return self.token 
        else:
            self.error(fr"Login Failed: {login_response}")
        

    async def logout(self):
        self.config.read(str(self.config_file_path))
        self.token = self.config["AUTH"]["token"]

        if self.token is None:
            raise ValueError("token Is Empty")

        LOGOUT_URL = fr"{self.url}/marketdata/auth/logout"
        headers = {
            "Content-Type": "application/json",
            "authorization": self.token
        }

        response = rqs.delete(LOGOUT_URL, 
                              headers = headers)
        if response.status_code == 200:
            print("Successfully Logged Out")
            self.info(fr"Successfully Logged Out | Logout Response: {response}")
        else:
            print(fr"Unsuccessfully Logged Out | Logout Response: {response}")
            self.info(fr"Unsuccessfully Logged Out | Logout Response: {response}")
        
    
    