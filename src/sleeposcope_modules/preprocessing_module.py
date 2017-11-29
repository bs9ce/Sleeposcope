#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
@author: Bahar Sharafi
This module contains all functions to be used in the sleeposcope project.
"""

import pandas as pd
import numpy as np
from sleeposcope_modules.sanity_check_module import check_file_df_content
from sleeposcope_modules.sanity_check_module import get_file_names_if_they_exist
from sleeposcope_modules.sanity_check_module import FileIsNotCorrectDataFileError
from sleeposcope_modules.sanity_check_module import check_if_data_frame_is_valid
from sleeposcope_modules.sanity_check_module import DateTimeColumnIsDefectiveError
from sleeposcope_modules.sanity_check_module import DataFrameContainsNans

def read_data(subject_folder_path):
    """ Reads in a number of csv files and returns a DataFrame containing the 
    concatenated data.
    
        Args: 
            subject_folder_path: str: Full or relative path to the csv data
            files
        Returns:
            subject_df: pandas DataFrame containing all data from current 
            subject, with garbage rows and unnecessary columns removed. The
            columns in this are:
                meas_sig_str: signal strength values (no units given, likely in
                mV) measured at 1 Hz
                date_time: time in local time
                secs: number of seconds passed from the beginning of the
                recording.
    """
    # Get the file paths:
    file_names = get_file_names_if_they_exist(subject_folder_path)

    # Create a list of DataFrames from each file to be concatenated into a
    # single DataFrame, subject_df:
    file_dfs = []
    for file_name in file_names:
        file_df = pd.read_csv(file_name)
        # If file content is incorrect, do not append to list. If the file was
        # not imported then raise the exception:
        try:
            check_file_df_content(file_df)
            file_dfs.append(file_df)
        except FileIsNotCorrectDataFileError:
            print("""{0} is incorrect and its contents will not be appended to 
                the subject data.""".format(file_name))

    subject_df = pd.concat(file_dfs, ignore_index=True)
    check_if_data_frame_is_valid(subject_df)


    # Discard garbage rows and columns:
    subject_df = clean_up(subject_df)
    # Convert date and time to pandas datetime in local time:
    subject_df['date_time'] = convert_to_local_time(subject_df.time)
    del subject_df['time']

    # Convert signal strength (meas_sig_str):
    subject_df.loc[:, 'meas_sig_str'] = pd.to_numeric(
        subject_df['meas_sig_str'])

    # Fail this step is subject_df is None, empty, or contains NaNs
    check_if_data_frame_is_valid(subject_df)

    return subject_df


def clean_up(subject_df):
    """ Removes garbage rows and columns. Each subject file include garbage 
    rows were the heading is repeated. In addition, there is a useless column 
    header called 'name'. 
    
        Args: 
            subject_df: pandas DataFrame containing all data from current 
            subject
        Returns:
            subject_df   
    """

    # Remove the 'name' column:
    del subject_df['name']

    # Remove garbage rows (they repeat the column headers so 'time' in column 
    # 'time' and so on)
    mask = subject_df['time'] != 'time'
    subject_df = subject_df[mask]

    # Fail this step is subject_df is None, empty, or contains NaNs
    check_if_data_frame_is_valid(subject_df)

    return subject_df


def convert_to_local_time(time_stamps):
    """Convert time-stamps from csv files to pandas datetimes in local time.

        Args: 
            time_stamps: a Pandas series containing time-stamps that are 
            character strings of the form: yyyy-mm-ddThh:mm:ssZ, for instance 
            2017-05-25T23:59:59Z
        Returns:
            date_time: a Pandas series containing pandas datetime objects in 
            local time     
    """

    # Remove letters Z and T:
    date_time = time_stamps.apply(
        lambda x: x.replace('Z', '').replace('T', ' '))
    # Convert to pandas timestamp:
    try:
        date_time = pd.to_datetime(date_time, utc=True)
    except ValueError:
        raise DateTimeColumnIsDefectiveError("The date time format in csv file "
                                             "is incorrect")
    # Localize to UTC time zone:
    date_time = date_time.apply(lambda x: x.tz_localize('UTC'))
    # Convert time zone:
    date_time = date_time.apply(lambda x: x.tz_convert('US/Pacific'))


    return date_time


def num_seconds(pandas_time_stamp):
    """Creates a series containing number of seconds for each record relative
    to the first time stamp recorded (in pandas_time_stamp)
    
        Args:
            pandas_time_stamp: Pandas series containing Pandas
        Returns:
            secs: Pandas series containing integers
    """

    secs = pandas_time_stamp - min(pandas_time_stamp)
    secs = secs.apply(lambda x: int(x.total_seconds()))

    return secs


def fill_missing_data(subject_df):
    """ Adds in missing seconds by re-indexing to continuous seconds. Fills in
    time stamps and signal strength for those missing seconds 
         
         Args:
             subject_df: A Pandas DataFrame: It includes missing seconds 
             ('date_time' is discontinuous).
         Returns:
             full_subject_df: A pandas DataFrame: It has no missing seconds and
             is indexed by seconds from the beginning of the recording.
    """

    # Set indices to seconds from the beginning of time (data is at Hz so there
    # is a record or row per second)
    secs = num_seconds(subject_df['date_time'])
    subject_df['secs'] = secs
    subject_df = subject_df.set_index('secs')

    # Continuous seconds from the begining to the end of the recording:     
    continuous_secs = set(range(0, max(subject_df.index)))
    # Re-index the DataFrame, this adds in a row for each missing second and 
    # fills it with NaNs:
    full_subject_df = subject_df.reindex(continuous_secs)
    # These are the seconds missing from the data:
    missing_secs = continuous_secs - set(secs)

    if len(missing_secs) == 0:
        full_subject_df = subject_df
    else:
        missing_secs = np.array(list(missing_secs)).astype(int)
        # Fill missing time_stamps:
        full_subject_df = fill_missing_time_stamps(full_subject_df, missing_secs)

        # Fill missing signal (If missing signal length is less than 5 minute
        # copies from the previous segment, else leaves it as NaNs.) :
        full_subject_df = fill_missing_signal_values(full_subject_df, missing_secs)

    # Abort here if full_subject_df is None or empty:
    try:
        check_if_data_frame_is_valid(full_subject_df)
    except DataFrameContainsNans:
        pass

    return full_subject_df


def fill_missing_time_stamps(full_subject_df, missing_secs):



    """ Creates time stamps for the missing seconds and writes those into the 
    DataFrame in place of NaNs.
    
        Args: 
            full_subject_df: A Pandas DataFrame. The indices are continious 
            seconds from the beginning of the recording filled with NaNs for 
            all rows were the signal was dropped.
            
            missing_secs: A numpy array containing the seconds were signal was 
            dropped. 
        Returns: 
            full_subject_df: A pandas DataFrame, the date_time for the 
            missing_secs has been filled with the correct time. 
    """

    convert_secs_to_time_stamp_vectorized = np.vectorize(
        lambda x: pd.Timedelta(x, unit='s'))
    missing_times = min(full_subject_df.date_time) + \
                    convert_secs_to_time_stamp_vectorized(missing_secs)
    full_subject_df.loc[missing_secs, 'date_time'] = missing_times

    return full_subject_df


def fill_missing_signal_values(full_subject_df, missing_secs):
    """If missing signal length is less than 5 minute copies from the previous 
    segment, else leaves it as NaNs.
    
        Args: 
            full_subject_df: A Pandas DataFrame: The indices are continious 
            seconds from the beginning of the recording filled with NaNs for 
            all rows were the signal was dropped.
            
            missing_secs: A numpy array containing the seconds were signal was 
            dropped
        Returns: 
            full_subject_df: A pandas DataFrame: The meas_sig_str for the 
            missing_secs has been filled with copied values. 
    """

    # Create a new DataFrame, sig_str_df, to be used for identifying missing
    # blocks of signal and copying from previous blocks of recorded signal:
    sig_str_df = full_subject_df['meas_sig_str'].to_frame()
    # find blocks with missing signal:
    sig_str_df['newcol'] = 0
    sig_str_df.loc[missing_secs, 'newcol'] = -1000
    sig_str_df['block'] = (sig_str_df.newcol.shift(1) != sig_str_df.newcol). \
        astype(int).cumsum()

    block_groups = sig_str_df.groupby(['block', 'newcol'], axis=0)

    missing_seg_length = []
    sig_str_df['block_length'] = 0
    sig_str_df['copy_sig'] = np.NaN
    for (i, j), group in block_groups:
        if j == -1000:
            missing_seg_length = missing_seg_length + [group.shape[0]]
            missing_block_inds = group.index
            sig_str_df.loc[missing_block_inds, 'block_length'] = group.shape[0]
            if group.shape[0] <= 5 * 60:
                shape_filler_ind = missing_block_inds - len(missing_block_inds)
                filler = np.array(sig_str_df.loc[shape_filler_ind, \
                                                 'meas_sig_str'])
                sig_str_df.loc[missing_block_inds, 'copy_sig'] = filler

                full_subject_df.loc[missing_secs, 'meas_sig_str'] = \
                    sig_str_df.loc[missing_secs, 'copy_sig']

    return full_subject_df


def divide_to_24_hour_periods(full_subject_df):
    """ Divides up the time to 24 hour periods. The signal is continously
    recorded for several days and nights. 24 hour periods from noon to the
    following noon are appropriate chunks to seperate the data.

        Args:
            full_subject_df: A pandas dataframe: It has no missing seconds and
            is indexed by seconds from the beginning of the recording.
        returns:
            full_subject_df: It has one additional column compared to the
            original DataDrame, 'day_nums', that specifies the 24 hour period from the start
            of the recording.  Each 24 hour period starts at noon. If the
            recoding starts at a time different that noon, the period 1 is
            shorter and ends at the following noon. Similarly, the last period
            may be truncated.
    """
    date_time = full_subject_df.date_time
    # Initialize day_nums:
    day_nums = pd.Series(index=date_time.index)

    # Find the onsets of each 24 hour period:
    day_onsets = [date_time[0]]
    end_of_time = date_time[len(date_time) - 1]

    if date_time.loc[0].hour < 12:
        next_day_onset = pd.to_datetime(str(date_time[0].date()) + ' 12:00:00')
    else:
        next_day_onset = pd.to_datetime(
            str(date_time[0].date()) + ' 12:00:00') + pd.Timedelta('24 hours')
    next_day_onset = next_day_onset.tz_localize('US/Pacific')

    while next_day_onset < end_of_time:
        day_onsets.append(next_day_onset)
        next_day_onset = next_day_onset + pd.Timedelta('24 hours')

    # Assign records to each day:
    for d in range(len(day_onsets)):
        tomorrow = day_onsets[d+1] if (d + 1 < len(day_onsets) - 1) else next_day_onset
        day_mask = (date_time >= day_onsets[d]) & (date_time < tomorrow)
        day_nums[day_mask] = d + 1

    full_subject_df['num_days'] = np.asarray(day_nums)

    return full_subject_df
