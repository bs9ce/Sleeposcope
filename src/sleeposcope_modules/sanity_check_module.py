#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
@author: Bahar Sharafi
This module contains all functions to be used in the sleeposcope project.
"""
# Libraries and modules to import:
import pandas as pd
from glob import glob
import os.path


class DateTimeColumnIsDefectiveError(Exception): pass


class SubjectExistsInDBError(Exception): pass


class DataFileOrFolderDoesNotExistError(Exception): pass


class DataFileIsDefectiveError(Exception): pass


class FileIsNotCorrectDataFileError(Exception): pass


class QuerryFailedError(Exception): pass


class DBConnectionFailedOrNonexistentError(Exception): pass


class FailedToReadCSVError(Exception): pass


class  DataFrameIsEmpty(Exception): pass


class  DataFrameIsNone(Exception): pass


class DataFrameContainsNans(Exception): pass


class Subject_num_does_not_match_subject_files_path(Exception): pass


def check_if_subject_num_matches_subject_files_path(subject_num,
                                                    subject_files_path):
    """Checks if subject_files_path matches subject_num are inconsistent.
    subject_files_path must end in subject_num, otherwise, raises an exception.

    Args:
        subject_num: int
        subject_files_path: character string, full or relative path
    Returns: Nothing
    """

    subject_num_str = str(subject_num)
    if subject_files_path[-len(subject_num_str):] != subject_num_str:
        raise Subject_num_does_not_match_subject_files_path("subject_num does "
                                                            "not match "
                                                            "subject folder")


def check_file_df_content(file_df):
    """Checks if the content of DataFrame read from the csv data file
    consistent with the expected format (i.e., it is mostly full, has no NaNs,
    it has the following column headers), if not, then it raises an exception.
    Expected column headers:
        'name', 'time', 'meas_sig_str', 'status'

    Args:
        file_df: Pandas DataFrame, imported from a csv data file
    Returns: Nothing
    """
    try:
        check_if_data_frame_is_valid(file_df)
    except DataFrameIsNone:
        raise FailedToReadCSVError("csv file content was not imported")
    except DataFrameIsEmpty:
        raise DataFileIsDefectiveError("csv file contains nothing")
    except DataFrameContainsNans:
        raise DataFileIsDefectiveError("csv file contains NaNs")

    if list(file_df.columns) != ['name', 'time', 'meas_sig_str', 'status']:
        raise FileIsNotCorrectDataFileError("""File content does not match 
            expected format""")


def check_if_data_frame_is_valid(df):
    """Checks if a DataFrame is None, empty, or contains NaNs and throws an
    exception in each case.
        Args:
            df: Pandas DataFrame
        Returns: Nothing
    """

    if df is None:
        raise DataFrameIsNone("DataFrame is None")
    elif len(df) == 0:
        raise DataFrameIsEmpty("DataFrame is empty")
    elif df.isnull().values.any():
        raise DataFrameContainsNans("DataFrame contains NaNs")


def get_file_names_if_they_exist(subject_folder_path):
    """ Checks that that csv files exist at the given path and returns their
    names. Otherwise, it throws an exception.

        Args:
            subject_folder_path: str: Full or relative path to the csv data
            files
        Returns:
            data_file_name_paths: list of character strings: containing path
            and file names of the csv files.
    """

    if subject_folder_path[-1] != 1:
        subject_folder_path = subject_folder_path + '/'

    if not os.path.exists(subject_folder_path):
        raise DataFileOrFolderDoesNotExistError(
            "Subject folder does not exist.")

    data_file_name_paths = glob('{0}*.csv'.format(subject_folder_path))

    if len(data_file_name_paths) == 0:
        raise DataFileOrFolderDoesNotExistError(
            "There are no data files in this folder")
    else:
        return data_file_name_paths


def check_engine(engine):
    """ Throws an exception if sqlalchemy engine is none.
    
        Args: 
           engine: sqlalchemy.engine.base.Engine: Database connection
        Returns:
            None 
    """
   
    if engine is None:
            raise DBConnectionFaliedOrNonexistentError("""Connection engine 
                                                   nonexistent""")


def check_if_subject_already_in_table(subject_num, TABLE_NAME, engine):
    """ Throws an exception if current subject data is already in the table
    
        Args: 
            subject_num: int
            TABLE_NAME: str: global variable
            engine: sqlalchemy.engine.base.Engine: Database connection
           
        Returns:
            None 
    """

    
    subject_exists_querry = """SELECT * FROM all_subjects_table WHERE 
    subject_num =""" + str(subject_num) + ' limit 1;'
    
    if does_table_exist_in_db(TABLE_NAME, engine):
        current_subject_data_in_db = pd.read_sql_query(
                subject_exists_querry,engine)
        
        if (current_subject_data_in_db is not None) and \
                len(current_subject_data_in_db) > 0:
            raise SubjectExistsInDBError(
                "This subject has already been added to the database!")
          
            
def does_table_exist_in_db(TABLE_NAME, engine):
    """ Determines whether a given table exists in database.
    
        Args: 
           TABLE_NAME: str
           engine: sqlalchemy.engine.base.Engine: Database connection
        Returns:
            table_exists: Bool
    """
    
    table_exists_querry = """
    SELECT EXISTS (
       SELECT 1
       FROM   information_schema.tables 
       WHERE  table_schema = 'public'
       AND    table_name = 'all_subjects_table'
       );
    """    
            
    table_exists = pd.read_sql_query(table_exists_querry, engine)
    
    if table_exists is None:
        raise QuerryFailedError("""Could not check whether data table exists in 
                                database""")
    else:
        return table_exists.iloc[0,0]
    
    