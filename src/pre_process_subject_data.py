#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Bahar Sharafi
Run this script from command line, for example to input subject_num = 2:
    
    'python pre_process_subject_data.py --subject_num 2 --subject_files_path
        ../data/Subject2'
    Args:
        subject_num: int: Current subject number
        
        subject_files_path: str: Directory were subject files are
        stored, full or relative path.
    
This scripts reads in the data for an *individual* subject (i.e., one subject 
at a time) from a number of csv files, creates a pandas dataframe and adds the 
current subject's data to an existing database specified as DB_NAME containing
other subjects' data (if the database does not exist, this script will create
it). Data for each subject are stored in a separate folder (e.g.Subject1,
Subject2) as a number of csv files.

The script first checks to see if the current subject is already in the 
sleeposcope database. If so, it raises an exceptions and aborts  mission.
"""

# Import the following modules:

from sleeposcope_modules.preprocessing_module import read_data, \
    fill_missing_data, divide_to_24_hour_periods
from sleeposcope_modules.sanity_check_module import \
    check_if_subject_already_in_table, get_file_names_if_they_exist, \
    check_if_subject_num_matches_subject_files_path

from sleeposcope_modules.talk_to_sql_module import connect_to_sql_database
from sleeposcope_modules.talk_to_sql_module import write_to_sql_database
import argparse

def main():
    
    # global variables
    DB_NAME = 'sleeposcope40'
    DB_USER_NAME = 'baharsharafi'
    DB_PSWD = 'sleeposcope'
    TABLE_NAME = 'all_subjects_table'
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--subject_num", type=int)
    parser.add_argument("--subject_files_path", type=str)
    args = parser.parse_args()
    subject_num = args.subject_num
    subject_files_path = args.subject_files_path

    # Check if subject_files_path ends in subject_num (it should!):
    check_if_subject_num_matches_subject_files_path(subject_num,
                                                    subject_files_path)

    # Create connection to PostgresSQL database:
    (engine, database_already_existed) = connect_to_sql_database(
            DB_USER_NAME, DB_PSWD, DB_NAME)
    
    # Check if subject is already in table (and if so abort):
    if database_already_existed:
        check_if_subject_already_in_table(subject_num, TABLE_NAME, engine)

    # Read in subject data:
    subject_data = read_data(subject_files_path)
    # Fill in missing data:
    subject_data = fill_missing_data(subject_data)
    # Divide the data to 'days' of recording:
    subject_data = divide_to_24_hour_periods(subject_data)
    
    # Add subject num and drop date_time:
    del subject_data['date_time']
    subject_data['subject_num'] = subject_num
    
    # Add subject data to all_subject_table in sleeposcope database:
    write_to_sql_database(subject_data, engine, TABLE_NAME)

    
if __name__ == '__main__':
    main()