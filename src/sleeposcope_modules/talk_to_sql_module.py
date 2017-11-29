#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
@author: Bahar Sharafi
This module contains functions to connect and write to a PostgresSQL database.
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sleeposcope_modules.sanity_check_module import check_engine

def connect_to_sql_database(DB_USER_NAME, DB_PSWD, DB_NAME):
    """ Creates a connection to a PostgreSQL database.

        Args:
    """

    engine = create_engine(
            'postgresql://%s:%s@localhost/%s'%(DB_USER_NAME, DB_PSWD, DB_NAME))
    
    # Check that the connection was established:
    check_engine(engine)
 
    # Create database if it does not exist
    if not database_exists(engine.url):
        db_already_exists = False
        create_database(engine.url)
    else:
        db_already_exists = True
        
    return engine, db_already_exists


def write_to_sql_database(subject_data, engine, TABLE_NAME):
    subject_data.to_sql(TABLE_NAME, engine, if_exists='append')