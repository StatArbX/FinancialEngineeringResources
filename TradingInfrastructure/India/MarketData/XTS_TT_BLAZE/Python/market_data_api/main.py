import requests as rqs
import json 
import configparser





"""
Note: In the following code the variables `section_header` & `section_item_name` are passed to get 
the header in the ini file and the key associated under that header.
"""

class MarketDataAPIFunctions:
    def __init__(self, 
                 url = "https://ttblaze.iifl.com", 
                 secretKey = None,
                 apiKey = None):        
        self.url = url
        self.secretKey = secretKey
        self.apiKey = apiKey
        self.auth_token = None
        self.exchangeSegment = None
        self.xtsMessageCode = None
        self.publishFormat = None
        self.broadCastMode = None
        self.instrumentType = None
        self.config = configparser.ConfigParser()
        self.config_file_path = "./login.ini"

    def ClientConfigResponse(self,                  
                            section_header : str, #this is the title of the section ini file
                            section_item_name : str): #this is the name of the item in the ini file section
        #clientself.config
        self.config.read(self.config_file_path)
        self.token = self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"   

        CLIENT_CONFIG_URL = fr"{self.url}/apimarketdata/config/clientConfig"
        header_client_config = {
            'Content-Type': 'application/json',
            'authorization': str(self.token)
        }
        response_client_config = rqs.get(url = CLIENT_CONFIG_URL, 
                                          headers = header_client_config)
        print(response_client_config)
        if response_client_config.status_code == 200:
            client_config_response_data = response_client_config.json()
            data = client_config_response_data.get('result') 
            # this might cause an error if so check the key name and change it
            self.exchangeSegment = client_config_response_data.get('exchangeSegments') 
            self.config['exchangeSegments'] = {key: int(value) for key, value in data['exchangeSegments'].items()}
            self.xtsMessageCode = client_config_response_data.get('xtsMessageCode') 
            self.config['xtsMessageCode'] = {key: int(value) for key, value in data['xtsMessageCode'].items()}
            self.publishFormat = client_config_response_data.get('publishFormat') 
            self.config['publishFormat'] = {'format': ','.join(data['publishFormat'])}
            self.broadCastMode = client_config_response_data.get('broadCastMode')
            self.config['broadCastMode'] = {'format': ','.join(data['broadCastMode'])} 
            self.instrumentType = client_config_response_data.get('instrumentType') 
            self.config['instrumentType'] = {key: str(value) for key, value in data['xtsMessageCode'].items()}

        else:
            print("CLIENTself.config REQUEST FAILED")

    #get index list

    def IndexList(self, 
                  exchangeSegment: int,
                  section_header : str, #this is the title of the section ini file
                  section_item_name : str): #this is the name of the item in the ini file section
        
        self.config.read(str(self.config_file_path))
        self.token =self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"        
        assert exchangeSegment is not None,"Exchange Segment Value cannot be set to None"
        INDEX_LIST_URL = fr"{self.url}/apimarketdata/instruments/indexlist"
        header_index_list = {
            'Content-Type': 'application/json',
            'authorization': str(self.token) 
        }
        payload_index_list = {
            'exchangeSegment': int(exchangeSegment)
        }
        response_index_list = rqs.get(url = INDEX_LIST_URL, 
                                      headers = header_index_list, 
                                      params = payload_index_list)
        if response_index_list.status_code == 200:
            index_list_data = response_index_list.json() 
            index_list_cash_market = index_list_data.get('result').get('indexList')
            return index_list_cash_market
        else:
            print("BAD INDEX LIST REQUEST")


    #get series list
    def GetSeries(self, 
                    exchangeSegment : int,
                    section_header : str, #this is the title of the section ini file
                    section_item_name : str): #this is the name of the item in the ini file section
        
        self.config.read(str(self.config_file_path))
        self.token =self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"        
        assert exchangeSegment is not None,"Exchange Segment Value cannot be set to None"

        SERIES_LIST_URL = fr"{self.url}/apimarketdata/instruments/instrument/series"
        header_series_list = {
            'Content-Type': 'application/json',
            'authorization': str(self.token) 
        }
        payload_series_list = {
            'exchangeSegment': int(exchangeSegment)
        }
        response_series_list = rqs.get(url = SERIES_LIST_URL, 
                                       headers = header_series_list, 
                                       params = payload_series_list)

        if response_series_list.status_code == 200:
            series_list_data = response_series_list.json()
            series_futures_options_list = series_list_data.get('result')
            return series_futures_options_list
        else:
            print("BAD REQUEST SERIES LIST")

    def GetExpiryDate(self,
                          exchangeSegment: int,
                          series: str, 
                          symbol: str,
                          section_header : str, #this is the title of the section ini file
                          section_item_name : str): #this is the name of the item in the ini file section
        
        self.config.read(str(self.config_file_path))

        self.token =self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"

        missing_params = []
    
        if series is None:
            missing_params.append("series")
        if symbol is None:
            missing_params.append("symbol")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")
        
        OPTIONS_EXPIRY_LIST_URL = "https://ttblaze.iifl.com/apimarketdata/instruments/instrument/expiryDate"  
        
        header_option_expiry_list = {
            'Content-Type': 'application/json',
            'authorization': str(self.token)
        }
        
        payload_option_expiry_list = {
            'exchangeSegment': int(exchangeSegment),
            'series': str(series),
            'symbol': str(symbol)
        }
        response_series_list = rqs.get(url = OPTIONS_EXPIRY_LIST_URL, 
                                       headers = header_option_expiry_list, 
                                       params = payload_option_expiry_list)
        if response_series_list.status_code == 200:
            series_list_data = response_series_list.json()
            return series_list_data
        else:
            print("BAD REQUEST SERIES LIST")

    def MasterData(self):
        self.config.read(str(self.config_file_path))
        self.token = self.config['AUTH']['token']
        
        MASTER_DATA_URL = "https://ttblaze.iifl.com/apimarketdata/instruments/master"
        
        header_master_list = {
            'Content-Type': 'application/json',
            'authorization': str(self.token)
        }
        
        payload_master_list = {
            'exchangeSegmentList': ["NSECM","NSECD","NSEFO"]
        }
        
        response_master_list = rqs.post(url = MASTER_DATA_URL, 
                                        headers = header_master_list, 
                                        json = payload_master_list)
        
        if response_master_list.status_code == 200:
            master_list_data = response_master_list.json()
            data = master_list_data['result']
            data_asset_classes = data.split('\n')
            asset_master_data = []
            data_header = [
                    "ExchangeSegment", "ExchangeInstrumentID", 
                    "InstrumentType", "Name", "Description",
                    "Series", "NameWithSeries", "InstrumentID",
                    "PriceBand.High", "PriceBand.Low",
                    "FreezeQty", "TickSize", "LotSize", 
                    "Multiplier", "UnderlyingInstrumentId",
                    "UnderlyingIndexName", "ContractExpiration", 
                    "StrikePrice", "OptionType",
                    "DisplayName", "PriceNumerator", 
                    "PriceDenominator", "DetailedDescription"
                    ]
        
            for asset in data_asset_classes:
                asset_values = asset.split('|')
                master_data_dict = dict(zip(data_header,asset_values))
                asset_master_data.append(master_data_dict)

            with open("./market_data_list.json","w") as json_file:
                json.dump(asset_master_data, json_file, indent = 4)
        else:
            print("MASTER DATA BAD REQUEST")


    def Quotes(self,
                    exchangeSegment : int,
                    exchangeInstrumentID : int,
                    xtsMessageCode : int, 
                    publishFormat : str,
                    section_header : str, #this is the title of the section ini file
                    section_item_name : str): #this is the name of the item in the ini file section
        
        self.config.read(str(self.config_file_path))

        self.token =self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"
        
        missing_params = []
    
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if exchangeInstrumentID is None:
            missing_params.append("exchangeInstrumentID")
        if xtsMessageCode is None:
            missing_params.append("xtsMessageCode")
        if publishFormat is None:
            missing_params.append("publishFormat")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")

        QUOTES_URL = "https://ttblaze.iifl.com/apimarketdata/instruments/quotes"

        headers_dict = {
            'Content-Type': 'application/json',
            'authorization': str(self.token)
        }
        payload_dict = {
            "intstruments": [
                {
                    "exchangeSegment": int(exchangeSegment),
                    "exchangeInstrumentID": int(exchangeInstrumentID)
                }
            ],
            "xtsMessageCode": int(xtsMessageCode),
            "publishFormat": str(publishFormat)
        }

        quotes_response = rqs.post(url = QUOTES_URL,
                                   headers = headers_dict,
                                   params = payload_dict)
        
        if quotes_response.status_code == 200:
            response_json = quotes_response.json()
            result = response_json.get('result')
            list_quotes_str = result["listQuotes"]

            list_quotes = json.loads(list_quotes_str)

            message_code = list_quotes["MessageCode"]
            exchange_segment = list_quotes["ExchangeSegment"]
            exchange_instrument_id = list_quotes["ExchangeInstrumentID"]

            bids = list_quotes["Bids"]
            for bid in bids:
                size = bid["Size"]
                price = bid["Price"]
                total_orders = bid["TotalOrders"]
                buy_back_market_maker = bid["BuyBackMarketMaker"]

                print(f"Bid - Size: {size}, Price: {price}, TotalOrders: {total_orders}, BuyBackMarketMaker: {buy_back_market_maker}")

            asks = list_quotes["Asks"]
            for ask in asks:
                size = ask["Size"]
                price = ask["Price"]
                total_orders = ask["TotalOrders"]
                buy_back_market_maker = ask["BuyBackMarketMaker"]

                print(f"Ask - Size: {size}, Price: {price}, TotalOrders: {total_orders}, BuyBackMarketMaker: {buy_back_market_maker}")

            touchline = list_quotes["Touchline"]
            last_traded_price = touchline["LastTradedPrice"]
            last_traded_quantity = touchline["LastTradedQunatity"]

            print(f"Last Traded Price: {last_traded_price}, Last Traded Quantity: {last_traded_quantity}")


        elif quotes_response.status_code != 200:
            print(f"Error in Quotes Response: {quotes_response.status_code}")
        else:
            print("Unidentifiable error occurred")



    def Subscription(self,
                    exchangeSegment : int,
                    exchangeInstrumentID : int,
                    xtsMessageCode: int,
                    section_header: str, #this is the title of the section ini file
                    section_item_name: str): #this is the name of the item in the ini file section
        
        self.config.read(str(self.config_file_path))

        self.token = self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"
        
        missing_params = []
        
    
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if exchangeInstrumentID is None:
            missing_params.append("exchangeInstrumentID")
        if xtsMessageCode is None:
            missing_params.append("message_code")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")

        SUBSCRIPTION_URL = "https://ttblaze.iifl.com/apimarketdata/instruments/subscription"

        headers_dict = {
            'Content-Type': 'application/json',
            'authorization': str(self.token)
        }
        payload_dict = {
            "intstruments": [
                {
                    "exchangeSegment": int(exchangeSegment),
                    "exchangeInstrumentID": int(exchangeInstrumentID)
                }
            ],
            "xtsMessageCode": int(xtsMessageCode),
        }

        subscription_response = rqs.post(url = SUBSCRIPTION_URL,
                                         headers = headers_dict,
                                         params = payload_dict)

        if subscription_response.status_code != 200:
            print(fr"Error in Subscription Response {subscription_response.status_code}")
            return response_data_dict
        elif subscription_response.status_code != 200:
            response_json = subscription_response.json()
            response_data_dict = response_json.get('result')
        else:
            print("Unidentifiable Error Occured")

    
    def Unsubscription(self,
                    exchangeSegment : int,
                    exchangeInstrumentID : int,
                    xtsMessageCode: int ,                        
                    section_header: str, #this is the title of the section ini file
                    section_item_name: str): #this is the name of the item in the ini file section
        
        self.config.read(str(self.config_file_path))

        self.token =self.config[section_header][section_item_name]
        
        assert self.token, "token should not be None but is None"
        
        missing_params = []
        
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if exchangeInstrumentID is None:
            missing_params.append("exchangeInstrumentID")
        if xtsMessageCode is None:
            missing_params.append("xtsMessageCode")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")


        UNSUBSCRIPTION_URL = "https://ttblaze.iifl.com/apimarketdata/instruments/subscription"

        headers_dict = {
            'Content-Type': 'application/json',
            'authorization': str(self.token)
        }
        payload_dict = {
            "intstruments": [
                {
                    "exchangeSegment": int(exchangeSegment),
                    "exchangeInstrumentID": int(exchangeInstrumentID)
                }
            ],
            "xtsMessageCode": int(xtsMessageCode),
        }

        unsubscription_response = rqs.put(url = UNSUBSCRIPTION_URL,
                                         headers = headers_dict,
                                         params = payload_dict)

        if unsubscription_response.status_code != 200:
            print(fr"Error in Subscription Response {unsubscription_response.status_code}")
        else:
            response_json = unsubscription_response.json()
            response_data_dict = response_json.get('result')

        return response_data_dict
    
    
    def GetEquitySymbol(self,
                        exchangeSegment : int,
                        series : str,
                        symbol : str,
                        section_header: str,
                        section_item_name: str):
        
        GET_EQUITY_SYMBOL_URL = fr"https://ttblaze.iifl.com/apimarketdata/instruments/symbol"
        
        self.config.read(str(self.config_file_path))
        self.token = self.config[section_header][section_item_name]
        assert self.token, "token should not be None but is None"

        missing_params = []
        
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if series is None:
            missing_params.append("series")
        if symbol is None:
            missing_params.append("symbol")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")
        
        headers_dict = {
            "Content-Type": "application/json",
            "authorization": str(self.token)
        }

        payload_dict = {
            "exchangeSegment": int(exchangeSegment),
            "series": str(series),
            "symbol": str(symbol)    
        }
        
        get_equity_symbol_response = rqs.get(url = GET_EQUITY_SYMBOL_URL,
                                             params = payload_dict,
                                             headers = headers_dict)

        if get_equity_symbol_response.status_code == 200:
            response_json = get_equity_symbol_response.json()
            response_data_dict = response_json.get('result')
            return response_data_dict
        elif get_equity_symbol_response.status_code != 200:
            print(f"Error in get_equity_symbol_response Response: {get_equity_symbol_response.status_code}")
        else:
            print("Unidentifiable error occurred")



    def GetFutureSymbol(self,
                        exchangeSegment : int,
                        series : str,
                        symbol : str,
                        expiryDate: str,
                        section_header: str,
                        section_item_name: str):
        
        GET_FUTURE_SYMBOL_URL = fr"https://ttblaze.iifl.com/apimarketdata/instruments/futureSymbol"
        
        self.config.read(str(self.config_file_path))
        self.token =self.config[section_header][section_item_name]
        assert self.token, "token should not be None but is None"

        missing_params = []
        
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if series is None:
            missing_params.append("series")
        if symbol is None:
            missing_params.append("symbol")
        if expiryDate is None:
            missing_params.append("expiryDate")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")
        
        headers_dict = {
            "Content-Type": "application/json",
            "authorization": str(self.token)
        }

        payload_dict = {
            "exchangeSegment": int(exchangeSegment),
            "series": str(series),
            "symbol": str(symbol)    
        }
        
        get_future_symbol_response = rqs.get(url = GET_FUTURE_SYMBOL_URL,
                                             params = payload_dict,
                                             headers = headers_dict)

        if get_future_symbol_response.status_code == 200:
            response_json = get_future_symbol_response.json()
            response_data_dict = response_json.get('result')
            return response_data_dict
        elif get_future_symbol_response.status_code != 200:
            print(f"Error in get_equity_symbol_response Response: {get_future_symbol_response.status_code}")
        else:
            print("Unidentifiable error occurred")


    def GetOptionSymbol(self,
                        exchangeSegment : int,
                        series : str,
                        symbol : str,
                        expiryDate: str,
                        optionType: str,
                        strikePrice: int,
                        section_header: str,
                        section_item_name: str):
        
        GET_OPTION_SYMBOL_URL = fr"https://ttblaze.iifl.com/apimarketdata/instruments/optionSymbol"
        
        self.config.read(str(self.config_file_path))
        self.token = self.config[section_header][section_item_name]
        assert self.token, "token should not be None but is None"

        missing_params = []
        
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if series is None:
            missing_params.append("series")
        if symbol is None:
            missing_params.append("symbol")
        if expiryDate is None:
            missing_params.append("expiryDate")
        if optionType is None:
            missing_params.append("optionType")
        if strikePrice is None:
            missing_params.append("strikePrice")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")
        
        headers_dict = {
            "Content-Type": "application/json",
            "authorization": str(self.token)
        }

        payload_dict = {
            "exchangeSegment": int(exchangeSegment),
            "series": str(series),
            "symbol": str(symbol)    
        }
        
        get_option_symbol_response = rqs.get(url = GET_OPTION_SYMBOL_URL,
                                             params = payload_dict,
                                             headers = headers_dict)

        if get_option_symbol_response.status_code == 200:
            response_json = get_option_symbol_response.json()
            response_data_dict = response_json.get('result')
            return response_data_dict
        elif get_option_symbol_response.status_code != 200:
            print(f"Error in get_option_symbol_response Response: {get_option_symbol_response.status_code}")
        else:
            print("Unidentifiable error occurred")

    def GetOptionType(self,
                        exchangeSegment : int,
                        series : str,
                        symbol : str,
                        expiryDate: str,
                        section_header: str,
                        section_item_name: str):
        
        GET_OPTION_TYPE_URL = fr"https://ttblaze.iifl.com/apimarketdata/instruments/optionType"
        
        self.config.read(str(self.config_file_path))
        self.token =self.config[section_header][section_item_name]
        assert self.token, "token should not be None but is None"

        missing_params = []
        
        if exchangeSegment is None:
            missing_params.append("exchangeSegment")
        if series is None:
            missing_params.append("series")
        if symbol is None:
            missing_params.append("symbol")
        if expiryDate is None:
            missing_params.append("expiryDate")
        
        if missing_params:
            raise ValueError(f"The following parameters cannot be None: {', '.join(missing_params)}")
        
        headers_dict = {
            "Content-Type": "application/json",
            "authorization": str(self.token)
        }

        payload_dict = {
            "exchangeSegment": int(exchangeSegment),
            "series": str(series),
            "symbol": str(symbol)    
        }
        
        get_option_type_response = rqs.get(url = GET_OPTION_TYPE_URL,
                                             params = payload_dict,
                                             headers = headers_dict)

        if get_option_type_response.status_code == 200:
            response_json = get_option_type_response.json()
            response_data_dict = response_json.get('result')
            return response_data_dict
        elif get_option_type_response.status_code != 200:
            print(f"Error in get_option_symbol_response Response: {get_option_type_response.status_code}")
        else:
            print("Unidentifiable error occurred")

    



