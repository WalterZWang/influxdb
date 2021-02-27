from os import path
import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import pandas as pd
import access

class WattTime():
    
    def __init__(self, loc):
        self.args = access.get_access('WattTime')
        self.username = self.args[0]
        self.password = self.args[1]
        self.token = self._login(self.username, self.password)
        self.loc = loc
        self.ba, self.ba_name = self._ba_from_loc(self.loc)
        print('The Balancing Authority for location {} is {}'.format(self.loc, self.ba_name))
        
    def update_location(self, loc):
        '''Input the location of area of interests, update the information of BA
        
        Params
        -------------------------------------------
        loc: location, list of [latitude, longitude]. Example: [37.87, -122.27] for Berkeley

        Return
        -------------------------------------------
        None
        ''' 
        self.loc = loc
        self.ba, self.ba_name = self._ba_from_loc(self.loc)
        print('The Balancing Authority for location {} is {}'.format(self.loc, self.ba_name))
    
    def percent_current(self):
        '''Retrieve the relative marginal emissions intenisty at the current time stamp for the current location
        
        Params
        -------------------------------------------
        None

        Return
        -------------------------------------------
        An integer between:
            0 (minimum MOER in the last two weeks i.e. clean), and 
            100 (maximum MOER in the last two weeks i.e. dirty) 
        Representing the relative realtime marginal emissions intensity.
        MOER: Marginal Operating Emissions Rate, measured in lbs CO2/MWh
        '''

        index_url = 'https://api2.watttime.org/index'
        headers = {'Authorization': 'Bearer {}'.format(self.token)}
        params = {'ba': self.ba}

        rsp = requests.get(index_url, headers=headers, params=params)
        print('Downloading relative marginal emissions intenisty for BA {} at the current time step'.format(self.ba))
        percent =  rsp.json()['percent']
        percent = int(percent)

        return percent

    def MOER_historical(self, start_time, final_time, mode='Test'):
        '''Retrieve historical data of Marginal Operating Emissions Rate (MOER) between 
        the start time and final time
        
        Params
        -------------------------------------------
        start_time: UTC time, str. Example: '2020-10-01 00:00:00'
        final_time: UTC time, str. Example: '2021-01-01 00:00:00'
        mode: 'Test' or 'Real'. During test mode, we do not have access to all data. Test mode is only for test only

        Return
        -------------------------------------------
        A dataframe with single column ['MOER'], with time as index. The time is in UTC time
            Unit of MOER is lbs of CO2 per MWh
        '''
        start_time_obj = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        final_time_obj = datetime.datetime.strptime(final_time, '%Y-%m-%d %H:%M:%S')
        start_time_iso = start_time_obj.replace(tzinfo=datetime.timezone.utc,
                                                microsecond=0).isoformat()
        final_time_iso = final_time_obj.replace(tzinfo=datetime.timezone.utc,
                                                microsecond=0).isoformat()
        data_url = 'https://api2.watttime.org/data'
        headers = {'Authorization': 'Bearer {}'.format(self.token)}
        if mode == 'Test':
            data_ba = 'CAISO_ZP26'
            print('Using the Test mode, the location you indicate is not available, only {} is available'.format(data_ba))
        else:
            data_ba = self.ba

        params = {'ba': data_ba, 
                  'starttime': start_time_iso, 'endtime': final_time_iso}
        rsp = requests.get(data_url, headers=headers, params=params)
        print('Downloading historical data for BA {} between {} and {} UTC'.format(data_ba, start_time, final_time))
        MOER_list = rsp.json() 
        MOER_df = self._parse_data(MOER_list)
        MOER_df.rename(columns={'value':'MOER'}, inplace=True)
        
        return MOER_df   
    
    def MOER_forecast(self, prediction_horizon=24, mode='Test'):
        '''Retrieve the forecast of Marginal Operating Emissions Rate (MOER) from now until the end of prediction horizon
        
        Params
        -------------------------------------------
        prediction_horizon: number of hours to predict ahead, float. Example: 24 for 1-day forecast
        mode: 'Test' or 'Real'. During test mode, we do not have access to all data. Test mode is only for test only

        Return
        -------------------------------------------
        A dataframe with single column ['MOER'], with time as index. The time is in UTC time
            Unit of MOER is lbs of CO2 per MWh
        '''
        
        start_time_obj = datetime.datetime.utcnow()
        final_time_obj = datetime.datetime.utcnow() + datetime.timedelta(hours = prediction_horizon) 
        start_time_iso = start_time_obj.replace(tzinfo=datetime.timezone.utc,
                                                microsecond=0).isoformat()
        final_time_iso = final_time_obj.replace(tzinfo=datetime.timezone.utc,
                                                microsecond=0).isoformat()
        
        forecast_url  = 'https://api2.watttime.org/forecast'
        headers = {'Authorization': 'Bearer {}'.format(self.token)}
        if mode == 'Test':
            data_ba = 'CAISO_NORTH'
            print('Using the Test mode, the location you indicate is not available, only {} is available'.format(data_ba))
        else:
            data_ba = self.ba

        params = {'ba': data_ba} 
        #          'starttime': start_time_iso, 'endtime': final_time_iso}
        # Current API only provides 24 hours prediction therefore there is no point to specify the startime and endtime
        # Also the startime much match 
        rsp = requests.get(forecast_url, headers=headers, params=params)
        print('Downloading forecast data for BA {} since {} for {} hours'.format(data_ba, start_time_iso, prediction_horizon))
        
        MOER_list = rsp.json()['forecast']
        MOER_df = self._parse_data(MOER_list)
        MOER_df.rename(columns={'value':'MOER'}, inplace=True)
        
        return MOER_df   

    def historical(self, mode='Test'):
        historical_url = 'https://api2.watttime.org/historical'
        headers = {'Authorization': 'Bearer {}'.format(self.token)}
        params = {'ba': self.ba}
        rsp = requests.get(historical_url, headers=headers, params=params)
        cur_dir = path.dirname(path.realpath(__file__))
        file_path = path.join(cur_dir, '{}_historical.zip'.format(self.ba))
        with open(file_path, 'wb') as fp:
            fp.write(rsp.content)

        print('Wrote historical data for {} to {}'.format(self.ba, file_path))

    def _login(self, username, password):
        '''Login with username and password, retrieve token.
        The token will used for data download
        '''
        login_url = 'https://api2.watttime.org/login'
        try:
            rsp = requests.get(login_url, auth=HTTPBasicAuth(username, password))
        except BaseException as e:
            print('There was an error making your login request: {}'.format(e))
            return None

        try:
            token = rsp.json()['token']
        except BaseException:
            print('There was an error logging in. The message returned from the '
                  'api is {}'.format(rsp.text))
            return None

        return token

    def _ba_from_loc(self, loc):
        '''Retrieve Balancing Authority from the latitude and longitude provided
        
        Params
        -------------------------------------------
        loc: location, list of [latitude, longitude]. Example: [37.87, -122.27] for Berkeley

        Return
        -------------------------------------------
        BA_abbrev: abbrevation of the BA
        BA_name: Name of the BA

        '''
        region_url = 'https://api2.watttime.org/v2/ba-from-loc'
        headers = {'Authorization': 'Bearer {}'.format(self.token)}
        params = {'latitude': loc[0], 'longitude': loc[1]}
        rsp=requests.get(region_url, headers=headers, params=params)
        BA = rsp.json()
        BA_abbrev = BA['abbrev']
        BA_name = BA['name']
        return BA_abbrev, BA_name    
    
    def _parse_data(self, data):
        '''Parse the data retrieved from WattTime server to pandas dataframe
        '''
        time_list = []
        data_list = []
        for item in data:
            time_list.append(item['point_time'])
            data_list.append(item['value'])
        data_df = pd.DataFrame(data_list, columns=['value'], index=time_list)
        data_df.index = pd.to_datetime(data_df.index)
        data_df.sort_index(ascending=True, kind='quicksort', inplace=True)
        
        return data_df
    

if __name__ == "__main__":
    ## Initiate an object for the downloader
    LOC = [37.87, -122.27]  # Berkeley, CA
    wattTime = WattTime(LOC)
    # Can use update_location method to update the location
    LOC2 = [42.37, -71.11]  # Cambridge, MA 
    wattTime.update_location(LOC2)        
    
    ## Retrieve the relative marginal emissions intenisty at the current time stamp
    # This function is free to all users
    percent_current = wattTime.percent_current()
    print('Relative marginal emissions intenisty for the current time step is {}'.format(percent_current))

    ## Download the historical MOER
    # This function needs Analyst account
    # Return dataframe
    START_UTC = '2020-12-01 00:00:00' 
    FINAL_UTC = '2020-12-05 12:00:00'
    moer_hist = wattTime.MOER_historical(START_UTC, FINAL_UTC, mode='Test')
    print(moer_hist)
    # Return None, save as zip file
    wattTime.historical(mode='Test')

    ## Download the forecast MOER
    # This function needs Analyst account
    # Return dataframe
    moer_forecast = wattTime.MOER_forecast(mode='Test')
    print(moer_forecast)