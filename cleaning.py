# -*- coding: utf-8 -*-
"""
Data cleaning functions

"""

import pandas as pd
import numpy as np
from collections import Counter
from interfacedb import setup_lbnl, database

import logging
logger = logging.getLogger('process.{0}'.format(__name__))

def find_all_continuous(df_list, length, min_noncontinuous=0):
    '''Find time intervals of list of continuous data greater than length.

    Parameters
    ----------
    df_list : list of pandas.DataFrame
        List of dataframes to check.
    length : int
        Minimum length of continuous data in seconds.
    min_noncontinuous : int
        Minimum length of time for noncontinuous data to be considered
        noncontinuous in seconds.

    Returns
    -------
    intervals_all : list
        List containing (start, end) time interval tuples.

    '''

    # Detect likely sample rate, add NAN to missing, and detect min sample rate
    df_list_rs = [];
    min_sample_rate = np.inf

    for df in df_list:
        likely_sample_rate, num_sample_rates = detect_likely_sample_rate(df)
        # Return empty list of error in detecting likely sample rate
        if likely_sample_rate is None:
            intervals_all = []

            return intervals_all

        # Resample to likely sample rate to add NAN to missing time
        df = df.resample(str(likely_sample_rate)+'s').asfreq().interpolate()
        df_list_rs.append(df)
        # Check if minimum sample rate
        if likely_sample_rate < min_sample_rate:
            min_sample_rate = likely_sample_rate
    # Report minimum sample rate
    logger.info('Minimum sample rate is {0}.'.format(min_sample_rate))
    # Create dataframe with all continuous finds
    df_continuous = pd.DataFrame()
    for df in df_list_rs:
        # Find intervals of continuous data for specific df
        logger.info('Finding intervals of continuous for {0}.'.format(df.columns.tolist()))
        intervals = find_continuous(df, length, min_noncontinuous=min_noncontinuous)
        if intervals:
            logger.info('Intervals found: {0}.'.format(intervals))
        else:
            logger.error('No intervals found for {}.'.format(df.columns))
            intervals_all = []

            return intervals_all

        # Create dataframe of continuous for specific df
        df_intervals = pd.DataFrame()
        for interval in intervals:
            index = pd.date_range(interval[0], interval[1], freq=str(min_sample_rate)+'s')
            data = np.ones(len(index))
            df_interval = pd.DataFrame(data=data, index=index, columns=df.columns.tolist())
            df_intervals = pd.concat((df_intervals, df_interval), axis=0)
        # Resample df_intervals to add NANs
        df_intervals = df_intervals.resample(str(likely_sample_rate)+'s').asfreq()
        # Add to dataframe of continuous of all dfs
        df_continuous = pd.concat((df_continuous, df_intervals), axis=1)

    # Find intervals of continuous data for all dfs
    intervals_all = find_continuous(df_continuous, length)

    return intervals_all

def find_continuous(df, length, whole=True, min_noncontinuous=0):
    '''Find the time intervals of continuous data greater than length.

    Parameters
    ----------
    df : pandas DataFrame
        Dataframe of data.  Must have datetimeindex.
    length : int
        Minimum length of time interval of continuous data in seconds.
    whole : bool, default=True
        True if consider whole df.  False if consider individual columns.
    min_noncontinuous : int
        Minimum length of time for noncontinuous data to be considered
        noncontinuous in seconds.

    Returns
    -------
    intervals : list if whole=True, dict if whole=False
        List containing (start, end) time interval tuples.
        If whole=False, {column header : [(start, end)]}.

    '''

    # Intialize
    if whole:
        # If whole dataframe
        df_null = df.isnull()
        ts_null = df_null.any(axis=1)
        intervals = _seperate_false_periods(ts_null, length=length, min_noncontinuous=min_noncontinuous)
    else:
        # If not whole df
        intervals = dict()
        for key in df.columns.values:
            ts_data = df[key]
            ts_null = ts_data.isnull()
            intervals_key = _seperate_false_periods(ts_null, length=length, min_noncontinuous=min_noncontinuous)
            # Fill dictionary
            intervals[key] = intervals_key

    return intervals

def check_continuous(df, whole=True):
    '''Check df contains continuous data.

    Parameters
    ----------
    df : pandas DataFrame
        Dataframe of data.  Must have datetimeindex.
    length : int
        Minimum length of time interval of continuous data in seconds.
    whole : bool, default=True
        True if consider whole df.  False if consider individual columns.

    Returns
    -------
    intervals : bool if whole=True, dict if whole=False
        True if continuous.  False if not continuous.
        If whole=False, {column header : bool}.

    '''

    # Intialize
    if whole:
        # If whole dataframe
        continuous = not(df.isnull().any(axis=1).any())
    else:
        # If not whole df
        continuous = dict()
        for key in df.columns.values:
            ts_data = df[key]
            # Fill dictionary
            continuous[key] = not(ts_data.isnull().any())

    return continuous

def _seperate_false_periods(ts, length=0, min_noncontinuous=0):
    '''Find time intervals where data is false.

    Parameters
    ----------
    ts : pandas timeseries
        Timeseries of boolean values.  Must have datetimeindex.
    length : int, default=0
        Only return intervals longer than this time in seconds.
    min_noncontinuous : int
        Minimum length of time for noncontinuous data to be considered
        noncontinuous in seconds.

    Returns
    -------
    time_periods_true : list
        List of (start, end) pairs of true intervals
    time_periods_false : list
        List of (start, end) pairs of false intervals

    '''

    # Find true and false time periods
    time_periods_true = [];
    time_periods_false = [];
    time_prev = ts.index.values[0]
    time_start_false = time_prev
    time_start_true = time_prev
    # For each time in ts
    for time in ts.index.values[1:]:
        # If is true
        if ts.loc[time]:
            # If one before is not true and its not the last time
            if not ts.loc[time_prev] and (time != ts.index.values[-1]):
                # Time is the start time of a true period
                time_start_true = time
                # Time previously is the end time of a false period
                time_end_false = time_prev
            # Or if one before is not true but it is last time
            elif (not ts.loc[time_prev]) and (time == ts.index.values[-1]):
                # Time previously is the end time of a false period
                time_end_false = time_prev
                # Store start and end times of false period
                time_periods_false.append((time_start_false, time_end_false))
            # Or if one before is true but it is last time
            elif ts.loc[time_prev] and time == ts.index.values[-1]:
                # Time currently is the end of the true time
                time_end_true = time
                # Store start and end times of true period
                time_periods_true.append((time_start_true, time_end_true))
                # Store start and end times of false period
                time_periods_false.append((time_start_false, time_end_false))

        # If is false
        else:
            # If one before is true and min_noncontinuous time is met
            if ts.loc[time_prev] and (time-time_start_true)/np.timedelta64(1,'s')>=min_noncontinuous:
                # If the series does not start true
                if time_start_true != ts.index.values[0]:
                    # Store start and end times of false period
                    time_periods_false.append((time_start_false, time_end_false))
                # Time previously is the end time of a true period
                time_end_true = time_prev
                # Store start and end times of period
                time_periods_true.append((time_start_true, time_end_true))
                # Time currently is the start time of a false period
                time_start_false = time
            # Or if one before is false but it is last time
            elif not ts.loc[time_prev] and time == ts.index.values[-1]:
                # Time currently is the end of the false time
                time_end_false = time
                time_periods_false.append((time_start_false, time_end_false))
            # Or if one before is true, it is last time, and min_noncontinuous time is not met
            elif ts.loc[time_prev] and time == ts.index.values[-1] and (time-time_start_true)/np.timedelta64(1,'s')<min_noncontinuous:
                # Time currently is the end of the false time
                time_end_false = time
                time_periods_false.append((time_start_false, time_end_false))
            # Or if one before is true but series started true
            elif ts.loc[time_prev] and time_start_true == ts.index.values[0]:
                # Time currently is the end of the false time
                time_start_false = time
        # Update previous time
        time_prev = time;
    # Check length of each false time period
    intervals = [];
    for time_period in time_periods_false:
        length_period = (time_period[1] - time_period[0]).astype('timedelta64[s]').astype(int)
        if length_period >= length:
            intervals.append(pd.to_datetime(time_period))

    return intervals

def detect_likely_sample_rate(df):
    '''Detect the likely sample rate of a dataframe.

    Parameters
    ----------
    df : DataFrame
        DataFrame with datetime index

    Returns
    -------
    likely_sample_rate : int
        Most common sample rate in DataFrame in seconds.
        None if error.
    num_sample_rates : int
        Number of sample rates detected.

    '''

    # Detect likely sampling interval
    time_diffs = [];
    time_prev = df.index.values[0]
    for time in df.index.values[1:]:
        # Keep track of time intervals
        time_diffs.append((time-time_prev).astype('timedelta64[s]').astype(int))
        # Update previous time
        time_prev = time;
    # Get likely sample rate and detect others
    num_sample_rates = len(Counter(time_diffs).items())
    try:
        likely_sample_rate = Counter(time_diffs).most_common(1)[0][0];
        logger.info('Likely sample rate of {0} is {1}.'.format(df.columns.tolist()[0], likely_sample_rate))
        if num_sample_rates > 2:
            logger.warning('Note that the number of detected sample rates is larger than 2.  This could imply significant missing data.  To be safe, making likely sample rate equal to 1 minute.')
            likely_sample_rate = 60
    except IndexError:
        likely_sample_rate = None
        logger.error('Error finding likely sample rate. Likely only one value in dataframe.')

    return likely_sample_rate, num_sample_rates

def fill_missing(fieldname, start_time_str, final_time_str):
    '''Collect the data and fill in the missed value with the mean of the same field of data at the same hour of the same dayOfWeek in the past 3 months,
       and then pushed into the database with the name 'fieldname_clean'

    Parameter
    -----------------
    fieldname: string
        The fieldname of data to be collected, should be consistent with the data in the database
    start_time_str: string
        Time to start to collect data, UTC
    final_time_str: string
        Time to finish to collect data, UTC
   '''
    db,dbname = setup_lbnl()
    time = pd.Timestamp.now(tz='UTC')
    # Find the data missed
    try:
        data = db.get_data(fieldname, dbname, start_time_str, final_time_str).resample('H').mean()
        data_toBeFilled = pd.DataFrame(index=pd.date_range(start = start_time_str, end = final_time_str, freq = 'H', tz='UTC'))
        data_toBeFilled = pd.concat([data_toBeFilled,data],axis=1)
        missing_time = data_toBeFilled[data_toBeFilled.isnull().values].index
    except:
        data_toBeFilled = pd.DataFrame(index=pd.date_range(start = start_time_str, end = final_time_str, freq = 'H', tz='UTC'))
        missing_time = data_toBeFilled.index
    # Define the historical table
    data_history = db.get_data(fieldname, dbname, (time - pd.Timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S'), final_time_str).resample('H').mean()
    data_history['hour'] = data_history.index.hour
    data_history['day'] = data_history.index.dayofweek
    # Fill in the missing data by table checking method
    for t in missing_time:
        data_table = data_history[data_history.day == t.dayofweek].groupby(['hour']).mean()
        data_toBeFilled.loc[t,fieldname] = float(data_table[data_table.index==t.hour].iloc[0,0])
    data_toBeFilled.columns = [fieldname+'_clean']
    db.write_data(data_toBeFilled.astype('float'),dbname)

if __name__ == "__main__":
    db,dbname = setup_lbnl()
    df_panel = db.get_data('sub590A14A', dbname, '2/20/2019', '3/15/2019')
    intervals = find_all_continuous([df_panel], 3600*24*4, min_noncontinuous=3600*5)
