"""
Please install these packages using pip
"""

import pandas as pd
import numpy as np 
from scipy.stats import norm
from datetime import datetime
import os
import csv
from concurrent.futures import ProcessPoolExecutor

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
These are all the basic formulas which dont need to be changed 
and will be all called into the InterviewTest class
"""
class BaseFormulas:
    def __init__(self):
        pass

    def appply_time_mask(self,
                         data = None,
                         start = None,
                         end = None):
        
        if not any([start,end,data]):
            raise ValueError("Empty parameters passed to the apply_time_mask function")
        
        mask = ((data['datetime'].dt.time >= start) & (data['datetime'].dt.time <= end))
        return data[mask]
    """
    This function is made to find the nearest atm strike price
    """
    def separate_option_types(self,
                              df = None):
        call_data = df[df['option_type'] == 'c']
        put_data = df[df['option_type'] == 'p']
        return (call_data, put_data)


    def group_df_daily(
                    self,
                    data = None
                    ):
        return data.groupby(pd.Grouper(key = 'datetime', freq = 'D'))
        
        
    def find_nearest_atm_strike(self, spot_price = None, strike_prices = None):
        return min(strike_prices, key = lambda x: abs(x-spot_price))
    """
    This function is made to find the closest strike price for the old dataset 
    it might be used once or twice later below to get the otms closes strikes
    """     
    def closest_strike_price(self, price, interval = 100):
        return int(round(price/interval)*interval)
    """
    I had added this since the first data given did not have the premium price, so i was 
    going to make the premiums column assuming a fixed volatility, since however the new dataset 
    contains the close price I will use that as a premiums reference
    """
    def black_scholes(self,
                    strike_price = None, 
                    spot_price = None, 
                    time_to_maturity = None, 
                    rate_of_interest = 7/100, 
                    sigma = 0.2, 
                    option_type = None):
    
        d1 = ((np.log(spot_price/strike_price)) + ((rate_of_interest)+((sigma**2)/2))*time_to_maturity)/(sigma*np.sqrt(time_to_maturity))
        d2 = d1 - sigma*(np.sqrt(time_to_maturity))

        if option_type == 'CE' or option_type == 'c':
            return float(((spot_price*norm.cdf(d1)) - (strike_price*np.exp(-rate_of_interest*time_to_maturity)*norm.cdf(d2))))
        elif option_type == 'PE' or option_type == 'p':
            return float(((strike_price*np.exp(-rate_of_interest*time_to_maturity)*norm.cdf(-d2)) - (spot_price*norm.cdf(-d1))))
        else:
            raise ValueError("Invalid Option Type")
      
    """
    This function is made to trim the timestamp in the expiry 
    such that that the option expires on that day at 15:30:00Hrs
    """
    def set_time_to_end_of_day(self,dt):
        return dt.replace(hour = 15, minute = 30, second = 00)
    """
    This function is made to compute the straddle price based on 
    an input of call price and put price
    """
    def compute_straddle_price(self, call_price = None, put_price = None):
        return call_price + put_price
    
    """
    This code makes a masked series for avg loos and avg giain 
    and returns it be put into a new column of a dataframe
    """
    def calculate_rsi(self,
                      data, 
                      window=30):
        if data.empty:
            raise ValueError
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)

        avg_gain = gain.rolling(window=window, min_periods=1).mean()
        avg_loss = loss.rolling(window=window, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
These are all the functionalities of objective two put into one class 
which will be inherited by the InterviewTest class
"""
class ObjTwoFormulas:
    def __init__(self):
        pass
    """
    This function is made to trim the timestamp in the expiry 
    such that that the option expires on that day at 15:30:00Hrs
    """
    def find_nearest_expiry_and_strike_min_obj_two(self,
                                grouped_data = None, 
                                time = None,
                                nearest_expiry = None,
                                nearest_strike_price = None,
                                start_trade_time = None,
                                end_trade_time = None,
                                ): 
        time = pd.to_datetime(str(time)).time()
        grouped_data = self.appply_time_mask(data = grouped_data,
                                            start = start_trade_time,
                                            end = end_trade_time,)
        
        grouped_data_per_minute_c = grouped_data[(grouped_data['expiry'] == nearest_expiry) & (grouped_data['option_type'] == 'c') & (grouped_data['strike_price'] == nearest_strike_price)].reset_index(drop = True)
        grouped_data_per_minute_p = grouped_data[(grouped_data['expiry'] == nearest_expiry) & (grouped_data['option_type'] == 'p') & (grouped_data['strike_price'] == nearest_strike_price)].reset_index(drop = True)

        if len(grouped_data_per_minute_c) != len(grouped_data_per_minute_p):
            raise KeyError("Length of call and put grouped data does not match")

        return (grouped_data_per_minute_c, grouped_data_per_minute_p)


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
These are all the functionalities of objective three put into one class 
which will be inherited by the InterviewTest class
"""

class ObjThreeFormulas(BaseFormulas):
    def __init__(self):
        super.__init__()

    """
    This finds the closes OTMs for my choice i chose the upper and lower 
    position, a strangle.
    """
    def find_otms_strikes_obj_three(self,
                          atm_strike = None,
                          interval = 100):
        
        if not any([atm_strike,interval]):
            raise ValueError("Empty parameters passed to the find_otms_strikes function")
        
        lower_otm = int(atm_strike - interval)
        upper_otm = int(atm_strike + interval)

        return (lower_otm, upper_otm)

    """
    This function gets the upper otm df, lower otm df and atm df
    """
    def slice_otms_obj_three(self,
                   df = None,
                   start_trade_time = None):
        
        if not any([start_trade_time, df]):
            raise ValueError("Items passed toslice_otms_obj_three() are empty")

        df.reset_index(drop = True, inplace = True)

        call_data = df[df['option_type'] == 'c']
        start_trade_data = call_data[call_data['datetime'].dt.time == start_trade_time]

        """
        Gets the nearest atm from the first row for the column nearest_atm
        """
        nearest_atm = start_trade_data.at[0, 'nearest_atm']

        try:
            lower_atm, upper_atm = self.find_otms_strikes_obj_three(atm_strike = nearest_atm)
        except Exception as e:
            raise IndexError("Error in slice_otms_obj_three() function")
        
        # to find the nearest expiry applying min on the expiry column 
        nearest_expiry = start_trade_data['expiry'].min()

        call_df = call_data[call_data['expiry'] == nearest_expiry].sort_values(by = 'datetime').reset_index(drop = True)
        atm_data = call_df[call_df['strike_price'] == nearest_atm].sort_values(by = 'datetime').reset_index(drop = True)
        low_otm_data = call_df[call_df['strike_price'] == lower_atm].sort_values(by = 'datetime').reset_index(drop = True)
        up_otm_data = call_df[call_df['strike_price'] == upper_atm].sort_values(by = 'datetime').reset_index(drop = True)

        return (atm_data, low_otm_data, up_otm_data)
    
    """
    Probably did not apply this since i did in the functions of interview class but can be
    used to spread out memory to increase latency with async/or without
    """
    def dictionary_for_each_day_obj_three(self,
                           grouped_obj = None):
        return {date: dataframe for date, dataframe in grouped_obj}

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
These are all the functionalities of objective four put into one class 
which will be inherited by the InterviewTest class
"""
class ObjFourFormulas(BaseFormulas):
    def __init__(self):
        super.__init__()
    
    def start_trade_df_obj_four(self,
                                df = None,
                     start_trade_time = pd.to_datetime("09:30:00").time()):
        df = df[(df['datetime'].dt.time == start_trade_time) & (df['expiry'] == df['expiry'].min())]
        nearest_atm_strike = int(df.iloc[0]['nearest_atm'])
        nearest_atm_expiry = df['expiry'].min()
        spot = df.iloc[0]['spot']
        return (nearest_atm_strike, nearest_atm_expiry, spot)
    
    """
    does the same work as the one in the previous class but is specific to the logic for 
    objective four
    """
    def filter_df_obj_four(self,
                           data = None,
                           start_trade_time = pd.to_datetime("09:30:00").time()):
        try:
            nearest_atm_strike ,nearest_atm_expiry, spot = self.start_trade_df_obj_four(df = data, 
                                                                                    start_trade_time = start_trade_time)
        except Exception as e:
            raise ValueError(fr"Calling start_trade_df_obj_four() from filter_df_obj_four() is giving an error: {e}")

        call_close = data[(data['strike_price'] == nearest_atm_strike) & (data['expiry'] == nearest_atm_expiry) & (data['datetime'].dt.time == start_trade_time) & (data['option_type'] == 'c')].iloc[0]['close']
        put_close = data[(data['strike_price'] == nearest_atm_strike) & (data['expiry'] == nearest_atm_expiry) & (data['datetime'].dt.time == start_trade_time) & (data['option_type'] == 'p')].iloc[0]['close']
        straddle_price = float(call_close + put_close)

        try:    
            call_strike = self.closest_strike_price(price = float((straddle_price*1.5) + spot))
            put_strike = self.closest_strike_price(price = float(spot-(straddle_price*1.5)))
        except Exception as e:
            raise ValueError(fr"Calling closest_strike_price() from filter_df_obj_four() is giving an error: {e}")

        nearest_expiry_df = data[data['expiry'] == nearest_atm_expiry]
        call_data = nearest_expiry_df[(nearest_expiry_df['option_type'] == 'c') & (nearest_expiry_df['strike_price'] == call_strike)].sort_values(by = 'datetime').reset_index(drop = True)
        put_data = nearest_expiry_df[(nearest_expiry_df['option_type'] == 'p') & (nearest_expiry_df['strike_price'] == put_strike)].sort_values(by = 'datetime').reset_index(drop = True)

        return (call_data, call_strike, put_data, put_strike)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
This class will execute the logic of the test inheriting all the functions from 
the parent class initialised in the class name
"""
class InterviewTest(ObjTwoFormulas,
                    ObjThreeFormulas,
                    ObjFourFormulas,
                    BaseFormulas):
    def __init__(self,
                 feather_file_path = "/Users/siddhanthmate/Desktop/AllFiles/Interview/calculations/combined_data_BN.feather"):
        
        """
        Here i am just inheriting the methods from 'Formulas' i thought this would be 
        better to read and understand.
        """
        super().__init__()
        """
        I have added this error handling since i use a UNIX based OS and sometimes, if this
        program is run a windows machine the path might throw errors and still continue running
        , and the output wont be the one you desired. So please check the path of the files you
        enter in the class.
        """
        
        try:
            self.df = pd.read_feather(str(feather_file_path))
        except FileNotFoundError:
            raise FileNotFoundError("File does not exist")
        except Exception as e:
            raise RuntimeError("Error occured while loading spot csv file")

        """
        This just initialises columns by vector applications
        The cleaning below before the loop is done to easily find the necessary rows by defining new ones:
        - strike_price_diff: this is the absolute value of the difference between each strike and the current spot,
        the lowest value is closest to the spot.
        - nearest_atm: applies a function to the spot to find the closed atm strike based on the interval of the 
        strikes and the spot
        - time_to_maturity: gets the float of the time in number of days to expiry from current datetime
        """
        try:
            self.df['expiry'] = pd.to_datetime(self.df['expiry'])
            self.df['date'] = pd.to_datetime(self.df['date'])
            self.df['expiry'] = self.df['expiry'].apply(self.set_time_to_end_of_day)
            self.df['time_to_maturity'] = (self.df['expiry']-self.df['date']).dt.total_seconds()/(3600*24)
            self.df['nearest_atm'] = self.df['spot'].apply(self.closest_strike_price)
            self.df['strike_price_diff'] = np.abs(self.df['strike_price'] - self.df['spot'])
        except Exception as e:
            raise KeyError("Something went wrong in class initialisation of Interview Test")

    def objective_one(self, 
                      start_trade_time = pd.to_datetime('09:30:00').time(),
                      end_trade_time = pd.to_datetime('15:15:00').time(),
                      ):
        
        """
        The entered start trade time and end time will be assumed as filters to ensure that only a 
        certain range in a trading day is used, please adjust this also accordingly, i have set the default
        as 930am to 315pm in the function, but can be changed while intialising
        """

        """
        This is the only function without inherited methods i just sliced the dataframe using row vectorisations
        could be wrong but this is the method effeceient in my experience but requires high testing
        """
        objective_one_data = []
        grouped_obj_one = self.df.groupby(pd.Grouper(key = 'datetime', freq = 'D'))
        for date, daily_spot_data in grouped_obj_one:
            nearest_atm_expiry = None
            calls_data = None
            puts_data = None
            data = None
            merged_df = None

            """
            Inherited method slices the dataframe to include data between 
            start time and end time only for each day
            """
            try:
                daily_spot_data = self.appply_time_mask(data = daily_spot_data,
                                                        start = start_trade_time,
                                                        end = end_trade_time)
            except Exception as e:
                print(fr"Error in calling appply_time_mask() within objective_one(): {e}")

            daily_spot_data.sort_values(by = 'datetime').reset_index(drop = True)

            nearest_atm_expiry = daily_spot_data['expiry'].min()
            data = daily_spot_data[(daily_spot_data['expiry'] == nearest_atm_expiry)]

            calls_data = data[(data['option_type'] == 'c') & (data['strike_price'] == data['nearest_atm'])].sort_values(by = 'datetime').reset_index(drop = True)
            puts_data = data[(data['option_type'] == 'p') & (data['strike_price'] == data['nearest_atm'])].sort_values(by = 'datetime').reset_index(drop = True)

            try:
                calls_data['ce_premium'] = calls_data['close']
                puts_data['pe_premium'] = puts_data['close']

                calls_data['ce_expiry'] = calls_data['expiry']
                puts_data['pe_expiry'] = puts_data['expiry']
                
                calls_data['underlying_ce'] = calls_data['spot']
                puts_data['underlying_pe'] = puts_data['spot']
            except Exception as e:
                raise KeyError(fr"Error in adding new columns: {e}")


            try:
                columns_to_drop = ['expiry', 'time_to_maturity', 'strike_price_diff', 
                                'date', 'close', 'nearest_atm']
                calls_data.drop(columns=columns_to_drop, 
                                axis=1, 
                                inplace=True)
                puts_data.drop(columns=columns_to_drop, 
                            axis=1, 
                            inplace=True)
            except Exception as e:
                raise KeyError(fr"Error in dropping columns: {e}")
            
            try:
                merged_df = pd.merge(calls_data, puts_data, on='datetime', how='inner')
                merged_df['straddle_premium'] = merged_df['ce_premium'] + merged_df['pe_premium']
                merged_df['symbol'] = merged_df['symbol_x']
                merged_df_columns_dropped = ['symbol_x', 'option_type_x', 'strike_price_x', 'spot_x', 
                                            'symbol_y', 'option_type_y', 'strike_price_y', 'spot_y']
                merged_df.drop(columns = merged_df_columns_dropped,
                                        axis = 1, 
                                        inplace = True)
            except Exception as e:
                raise ValueError(fr"Error in merging calls_data and puts_data: {e}")
            dt = date.strftime('%Y-%m-%d %H:%M:%S')
            data = {
                str(dt): merged_df
            }
            objective_one_data.append(data)
        

        output_dir = str(f"{os.getcwd()}/objective_one_output")

        os.makedirs(
                    output_dir,
                    exist_ok = True
                )

        for item in objective_one_data:
            for key, value in item.items():
                file_path = os.path.join(
                                        output_dir, 
                                        f"{key[0:10]}.csv"
                                        )
                value.to_csv(
                            file_path, 
                            index = False
                            )    
        try:
            output_dir = str(f"{os.getcwd()}/objective_one_output")
            os.makedirs(
                        output_dir,
                        exist_ok = True
                    )
            try:
                for item in objective_one_data:
                    for key, value in item.items():
                        file_path = os.path.join(
                                                output_dir, 
                                                f"{key[0:10]}.csv"
                                                )
                        value.to_csv(
                                    file_path, 
                                    index = False
                                    )    
            except KeyError as e:
                print(f"Error occurred in objective_one_output: {e}")  
            except Exception as e:
                print(f"Error occurred in objective_one_output: {e}")  

        except IOError as e:
            print(f"File I/O operations error {e} in objective_one_output")
        except Exception as e:
            print(f"Error occurred in objective_one_output: {e}")  
        
    
    def objective_two(self,
                      start_trade_time = pd.to_datetime("09:30:00").time(),
                      end_trade_time = pd.to_datetime("15:15:00").time()
                      ):
        """
        I have grouped into days first then over the time in each day group,
        this makes finding the nearest expiry and other factors easier,
        also I have added the start trade time and end trade time at the begining, these 
        are default values which can be changed according to the users needs
        """
        
        try:
            grouped = self.group_df_daily(data = self.df.copy(deep = True))
        except Exception as e:
            print(fr"Exception Error: {e}")
            raise ValueError("Empty grouped obj in objective three")
            
        objective_two = []
        objective_two_analysis = []

        for date, daily_spot_data in grouped:
            nearest_expiry = None
            nearest_strike_price = None
            call_data = None
            put_data = None

            daily_spot_data.reset_index(drop = True, inplace = True)

            if daily_spot_data.empty:
                print(fr"Data for {date} not present")
                continue

            nearest_expiry = daily_spot_data['expiry'].min()
            nearest_strike_price = daily_spot_data.iloc[0]['nearest_atm']

            kwargs = {
                    'grouped_data': daily_spot_data,
                    'time': date,
                    'nearest_expiry': nearest_expiry,
                    'nearest_strike_price': nearest_strike_price,
                    'start_trade_time': start_trade_time,
                    'end_trade_time': end_trade_time,
                }
            
            try:
                call_data, put_data = self.find_nearest_expiry_and_strike_min_obj_two(**kwargs)
            except Exception as e:
                print(fr"Error in calling find_nearest_expiry_and_strike_min_obj_two into objective_two")
            
            max_pnl = 0.0
            entry_straddle_premium = 0.0
            pnl = 0.0
            max_loss = 0.0
            min_pnl = 0.0

            df = None
            df_list = []

            for call_row, put_row in zip(call_data.itertuples(), put_data.itertuples()):
                if ((call_row.datetime == put_row.datetime) and (call_row.spot == put_row.spot) and (call_row.strike_price == put_row.strike_price) and (call_row.expiry == put_row.expiry)):
                    curr_time = call_row.datetime.time() or put_row.datetime.time()
                    curr_datetime = call_row.datetime or put_row.datetime
                    call_premium = float(call_row.close)
                    put_premium = float(put_row.close) 
                    spot = float(call_row.spot) or float(put_row.spot)
                    try:
                        straddle_price = self.compute_straddle_price(call_price = call_premium ,
                                                                    put_price = put_premium)
                    except Exception as e:
                        print(fr"Error in calling compute_straddle_price into objective_two function {e}")
                    
                    if (curr_time == pd.to_datetime("9:30:00").time()):
                        final_pnl = 0
                        trade_entry_strike = int(nearest_strike_price)
                        entry_straddle_premium = float(straddle_price)

                        max_pnl = float(entry_straddle_premium-0)
                        max_loss = float(nearest_strike_price - 0)
                        min_pnl = float(entry_straddle_premium - max_loss)
                        entry, net_position, exit = -1, 0, 0
                        
                    elif (pd.to_datetime("9:30:00").time() < curr_time < pd.to_datetime("15:15:00").time()):
                        final_pnl = float(entry_straddle_premium - straddle_price)
                        net_position = -1
                        entry = 0
                        exit = 0
                    elif (curr_time == pd.to_datetime("15:15:00").time()):
                        exit, net_position = 1, 0
                        final_pnl = float(entry_straddle_premium - straddle_price)
                        # analysis
                        dt = date.strftime('%Y-%m-%d %H:%M:%S')
                        summary_of_trading_day = {
                            str(dt[0:10]): {
                                'entry_straddle_premium': entry_straddle_premium,
                                'exit_premium': straddle_price,
                                'exit_pnl': final_pnl
                                }
                        }
                        objective_two_analysis.append(summary_of_trading_day)

                else:
                    raise ValueError("Mismatch in call and put row datetime")
                
                mtm = float(entry_straddle_premium - straddle_price)
        
                data_per_min = {
                    'datetime': curr_datetime,
                    'entry_strike': trade_entry_strike,
                    'time': curr_time,
                    'spot': spot,
                    'entry_straddle_premium': entry_straddle_premium,
                    'call_premium': call_premium,
                    'put_premium': put_premium,
                    'straddle_price': straddle_price,
                    'mtm': mtm,
                    'max_pnl': max_pnl,
                    'min_pnl': min_pnl,
                    'max_loss': max_loss,
                    'entry': entry,
                    'exit': exit,
                    'net_position': net_position,
                }

                df_list.append(data_per_min)
            
            df = pd.DataFrame(df_list).sort_values(by = 'datetime').reset_index(drop = True)
        
            objective_two.append({
                str(dt): df
            })

            df = None
            dt = None

        try:
            output_dir = str(f"{os.getcwd()}/objective_two_output/data")
            os.makedirs(output_dir,
                        exist_ok = True)
            try:
                for item in objective_two:
                    for key, value in item.items():
                        file_path = os.path.join(output_dir, 
                                                f"{key[0:10]}.csv")
                        value.to_csv(file_path, 
                                    index = False)  
            except KeyError as e:
                print(f"Error occurred in objective_two_output: {e}")  
            except Exception as e:
                print(f"Error occurred in objective_two_output: {e}")  

        except IOError as e:
            print(f"File I/O operations error {e} in objective_two_output")
        except Exception as e:
            print(f"Error occurred in objective_two_output: {e}")  
        
        
        try:
            output_dir = str(f"{os.getcwd()}/objective_two_output/pnl_analysis")
            os.makedirs(output_dir,
                        exist_ok = True)
            try:
                header = ['date', 'entry_straddle_premium', 'exit_premium' ,'exit_pnl']
                with open(fr"{output_dir}/obj_two_pnl_analysis.csv", mode='w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=header)
                    writer.writeheader()
                    
                    for item in objective_two_analysis:
                        for date, premiums in item.items():
                            row = {'date': date, **premiums}
                            writer.writerow(row)
            except KeyError as e:
                print(f"Error occurred in objective_two_output: {e}")  
            except Exception as e:
                print(f"Error occurred in objective_two_output: {e}")  
        except IOError as e:
            print(f"File I/O operations error {e} in objective_two_output")
        except Exception as e:
            print(f"Error occurred in objective_two_output: {e}")  
            

        return (objective_two, objective_two_analysis)
        
        
    def objective_three(self,
                        window_period = 30,
                        start_trade_time = pd.to_datetime("09:16:00").time()):
        
        try:
            grouped = self.group_df_daily(data = self.df.copy(deep = True))
        except Exception as e:
            print(fr"Exception Error: {e}")
            raise ValueError("Empty grouped obj in objective three")
        
        objective_three = []
        for datetime, daily_spot_data in grouped:
            if daily_spot_data.empty:
                print(fr"Data for {datetime} not present in objective_three()")
                continue
            
            try:
                atm_data, low_otm_data, up_otm_data = self.slice_otms_obj_three(df = daily_spot_data, 
                                                                        start_trade_time = start_trade_time)
            except Exception as e:
                print("Error in calling slice_otms_obj_three in objective_three")
                raise ModuleNotFoundError("Cannot call slice_otms_obj_three() function")
            
            for atm, lotm, uotm in zip(atm_data.itertuples(), low_otm_data.itertuples(), up_otm_data.itertuples()):
                if ((atm.datetime == lotm.datetime == uotm.datetime) and (lotm.spot == atm.spot == uotm.spot) and (uotm.strike_price != lotm.strike_price != atm.strike_price) and (lotm.expiry == uotm.expiry == atm.expiry)):
                    curr_time = atm.datetime.time() or lotm.datetime.time() or uotm.datetime.time()
                    curr_datetime = atm.datetime or uotm.datetime or lotm.datetime
                    lotm_premium = float(lotm.close)
                    atm_premium = float(atm.close) 
                    uotm_premium = float(uotm.close) 

                    spot = float(lotm.spot) or float(uotm.spot) or float(atm.spot)
                    expiry = lotm.expiry or uotm.expiry or atm.expiry

                    data_per_min = {
                        'index_datetime': curr_datetime,
                        'datetime': curr_datetime,
                        'time': curr_time,
                        'spot': spot,
                        'OTM1_CE_PREMIUM': lotm_premium,
                        'OTM1_CE_STRIKE': lotm.strike,
                        'OTM1_CE_STRIKE': uotm.strike,
                        'OTM2_CE_PREMIUM': uotm_premium,
                        'ATM_CE_PREMIUM': atm_premium,
                        'expiry': expiry
                    }
                    objective_three.append(data_per_min)
                    data_per_min = None
            daily_spot_data = None

        final_obj_three_df = pd.DataFrame(objective_three).sort_values(by = 'datetime').reset_index(drop = True)
        final_obj_three_df.set_index('index_datetime', inplace = True)
        
        # DMA Calculations based on window given
        final_obj_three_df['Banknifty_30_DMA'] = 0.0 
        final_obj_three_df['OTM1_CE_DMA'] = 0.0 
        final_obj_three_df['OTM2_CE_DMA'] = 0.0 
        final_obj_three_df['Banknifty_RSI'] = 0.0 
        final_obj_three_df['Banknifty_30_DMA'] = final_obj_three_df['spot'].rolling(window = window_period).mean()
        final_obj_three_df['OTM1_CE_DMA'] = final_obj_three_df['OTM1_CE_PREMIUM'].rolling(window = window_period).mean()
        final_obj_three_df['OTM2_CE_DMA'] = final_obj_three_df['OTM2_CE_PREMIUM'].rolling(window = window_period).mean()


        final_obj_three_df['Banknifty_RSI'] = self.calculate_rsi(final_obj_three_df['spot'])

        final_obj_three_df['Banknifty_30_DMA'] = final_obj_three_df['Banknifty_30_DMA'].apply(lambda x: 0 if pd.isna(x) else x)
        final_obj_three_df['OTM1_CE_DMA'] = final_obj_three_df['OTM1_CE_DMA'].apply(lambda x: 0 if pd.isna(x) else x)
        final_obj_three_df['OTM2_CE_DMA'] = final_obj_three_df['OTM2_CE_DMA'].apply(lambda x: 0 if pd.isna(x) else x)
        final_obj_three_df['Banknifty_RSI'] = final_obj_three_df['Banknifty_RSI'].apply(lambda x: 0 if pd.isna(x) else x)

        final_obj_three_df.sort_values(by = 'datetime')
        df_grouped = final_obj_three_df.groupby(pd.Grouper(key = 'datetime',
                                                   freq = 'D'))
        df_dict = {str(key.strftime('%Y-%m-%d %H:%M:%S')[0:10]): value for key, value in df_grouped}
        output_dir = str(f"{os.getcwd()}/objective_three_output")
        os.makedirs(output_dir,
                    exist_ok = True)
        for key, value in df_dict.items():
            file_path = os.path.join(output_dir, 
                                    f"{key[0:10]}.csv")
            value.to_csv(file_path, 
                        index = False)    
        
    def objective_four(self,
                       window_period = 30,
                       start_trade_time = pd.to_datetime("09:30:00").time(),
                       end_trade_time = pd.to_datetime("15:15:00").time(),
                       ):
        

        """
        I have grouped into days first then over the time in each day group,
        this makes finding the nearest expiry and other factors easier,
        also I have added the start trade time and end trade time at the begining, these 
        are default values which can be changed according to the users needs
        """
        try:
            grouped = self.group_df_daily(data = self.df.copy(deep = True))

        except Exception as e:
            print(fr"Exception Error: {e}")
            raise ValueError("Empty grouped obj in objective three")

        objective_four_data = []

        for datetime, data in grouped:
            call_data = None
            put_data = None 
            data.reset_index(drop = True, inplace = True)

            if data.empty:
                print(fr"Data for {datetime} not present in objective_four()")
                continue
            
            kwargs = {
                'data': data,
                'start_trade_time': start_trade_time
            }
            
            try:
                call_data, call_strike, put_data, put_strike = self.filter_df_obj_four(**kwargs)
            except Exception as e:
                print("Error occured in objective_four()")
                raise ModuleNotFoundError("Error occured while calling filter_df_obj_four()")

            for (call_row, put_row) in zip(call_data.itertuples(), put_data.itertuples()):
                if ((call_row.datetime == put_row.datetime) and (call_row.spot == put_row.spot) and (put_row.strike_price == put_strike) and (call_row.strike_price == call_strike) and (call_row.expiry == put_row.expiry)):
                    curr_time = call_row.datetime.time() or put_row.datetime.time()
                    curr_datetime = call_row.datetime or put_row.datetime
                    call_premium = float(call_row.close)
                    put_premium = float(put_row.close) 
                    curr_call_strike = int(call_row.strike_price)
                    curr_put_strike = int(put_row.strike_price)
                    spot = float(call_row.spot) or float(put_row.spot)
                    strangle_premium = float(call_premium + put_premium)
                    expiry = call_row.expiry or put_row.expiry
            
                    data_per_min = {
                        'datetime': curr_datetime,
                        'time': curr_time,
                        'spot': spot,
                        'expiry': expiry,
                        'ce_strike_price': curr_call_strike,
                        'pe_strike_price': curr_put_strike,
                        'call_premium': call_premium,
                        'put_premium': put_premium,
                        'strangle_premium': strangle_premium,
                    }

                    objective_four_data.append(data_per_min)
                data_per_min = None
            data = None
        
        final_obj_four_df = pd.DataFrame(objective_four_data).sort_values(by = 'datetime').reset_index(drop = True)
        final_obj_four_df['strangle_premium_dma'] = final_obj_four_df['strangle_premium'].rolling(window = int(window_period)).mean().apply(lambda x: 0 if pd.isna(x) else x)
 
        final_obj_four_df['entry'] = 0.0
        final_obj_four_df['net_position'] = 0.0
        final_obj_four_df['exit'] = 0.0
        final_obj_four_df['max_pnl'] = 0.0
        final_obj_four_df['mtm'] = 0.0
        final_obj_four_df['pnl'] = 0.0

        in_position = False

        for i in range(len(final_obj_four_df)):
            try:
                current_time = final_obj_four_df.at[i, 'datetime'].time()
                if start_trade_time <= current_time <= end_trade_time:
                    if current_time == end_trade_time:
                        if in_position:
                            final_obj_four_df.at[i, 'exit'] = 1
                            final_obj_four_df.at[i, 'net_position'] = 0
                            in_position = False
                            final_obj_four_df.at[i,'max_pnl'] = entry_premium
                            entry_premium = 0
                        continue

                    elif not in_position and final_obj_four_df.at[i, 'strangle_premium'] < final_obj_four_df.at[i, 'strangle_premium_dma']:
                        final_obj_four_df.at[i, 'entry'] = -1
                        final_obj_four_df.at[i, 'net_position'] = 0
                        in_position = True
                        entry_premium = final_obj_four_df.at[i, 'strangle_premium']
                        final_obj_four_df.at[i, 'max_pnl'] = entry_premium
                        final_obj_four_df.at[i, 'mtm'] = 0
                    elif in_position and final_obj_four_df.at[i, 'strangle_premium'] > final_obj_four_df.at[i, 'strangle_premium_dma']:
                        final_obj_four_df.at[i, 'exit'] = 1
                        final_obj_four_df.at[i, 'net_position'] = 0
                        final_obj_four_df.at[i, 'max_pnl'] = entry_premium
                        entry_premium = 0
                        in_position = False
                    elif in_position:
                        final_obj_four_df.at[i, 'net_position'] = -1
                        final_obj_four_df.at[i,'max_pnl'] = entry_premium
            except IndexError as e:
                raise IndexError(fr"Index error in final for loop in objective_four: {e}")


        """
        Using vectorisation to slice the data and applying a mask to get the 
        - mtm 
        - pnl
        """
        
        final_obj_four_df['mtm_values'] = final_obj_four_df['max_pnl'] - final_obj_four_df['strangle_premium']
        mask_mtm = (final_obj_four_df['net_position'] == -1) | (final_obj_four_df['net_position'] == 1)
        final_obj_four_df['mtm'] = final_obj_four_df['mtm_values'].where(mask_mtm, 0)
        final_obj_four_df.drop('mtm_values', axis=1, inplace=True)
        final_obj_four_df['pnl_values'] = final_obj_four_df['max_pnl'] - final_obj_four_df['strangle_premium']
        mask_pnl = (final_obj_four_df['exit'] == -1) | (final_obj_four_df['exit'] == 1)
        final_obj_four_df['pnl'] = final_obj_four_df['pnl_values'].where(mask_pnl, None)
        final_obj_four_df.drop('pnl_values', axis=1, inplace=True)


        final_obj_four_df['mtm'] = final_obj_four_df['mtm'].apply(lambda x: 0 if pd.isna(x) else x)
        final_obj_four_df['pnl'] = final_obj_four_df['pnl'].apply(lambda x: 0 if pd.isna(x) else x)
        final_obj_four_df['net_position'] = final_obj_four_df['net_position'].apply(lambda x: 0 if pd.isna(x) else x)
        

        """
        Grouping them to create files for each day
        """
        df_grouped = final_obj_four_df.groupby(pd.Grouper(key = 'datetime',
                                                    freq = 'D'))

        df_dict = {str(key.strftime('%Y-%m-%d %H:%M:%S')[0:10]): value for key, value in df_grouped}
        df_dict_analysis = [
            {str(key.strftime('%Y-%m-%d %H:%M:%S')[0:10]): float(group['pnl'].sum())}
            for key, group in df_grouped
        ]      

        try:
            output_dir = str(f"{os.getcwd()}/objective_four_output/data")
            os.makedirs(output_dir,
                    exist_ok = True)
       
            for key, value in df_dict.items():
                file_path = os.path.join(output_dir, 
                                        f"{key[0:10]}.csv")
                value.to_csv(file_path, 
                            index = False)
        except Exception as e:
            print(f"Error in creating data files for objective_four(): {e}")
        
        try:
            output_dir = str(f"{os.getcwd()}/objective_four_output/pnl_analysis")
            os.makedirs(output_dir,
                        exist_ok = True)
            flat_dict = {k: v for d in df_dict_analysis for k, v in d.items()}

            df_dict_analysis_df = pd.DataFrame.from_dict(flat_dict, orient='index', columns=['pnl']).reset_index()

            file_path = os.path.join(output_dir, 
                                    f"pnl_analysis_all_days.csv")
            df_dict_analysis_df.to_csv(file_path, 
                        index = False)
        except Exception as e:
            print(f"Error in creating df_dict_analysis_df files for objective_four(): {e}")


def execute_objective(method):
    file_path = "/Users/siddhanthmate/Desktop/AllFiles/Interview/calculations/combined_data_BN.feather"
    it = InterviewTest(feather_file_path = file_path)
    method(it)

def main():
    """
    Running this in thread to speed up the process
    """
    cpu_count = round(int(os.cpu_count())/2)
    objectives = [
        InterviewTest.objective_one,
        InterviewTest.objective_two,
        InterviewTest.objective_three,
        InterviewTest.objective_four
    ]
    
    with ProcessPoolExecutor(max_workers = cpu_count) as executor:
        executor.map(execute_objective, objectives)


if __name__ == "__main__":
    main()