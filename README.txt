The goal of this project was to use recordings from a mattress-embedded
accelerometer to determine whether a person is asleep, awake in bed, or not in
bed. Raw accelerometer signal is provided in csv format at 1Hz, alongside
ground-truth labels for the person's state. For more detail I refer you to:

                            bitly.com/Sleeposcope


Organization of this project:

├── README.md      <- The top-level README for developers using this project.
├── data		   <- This directory contains subject directories, each of which
│                     contains a number of .csv files. These are the raw data
│                     files. Actual data files are not provided here due to
│                     their proprietary nature.
│
└── src                <- Source code for use in this project.
    ├── sleeposcope_modules  <- modules used by scripts throughout the project
    │   └── __init__.py
    │   └── preprocessing_module.py
    │   └── sanity_check_module.py
    │   └── talk_to_sql_module.py  
    └── pre_process_subject_data.py <- Reads in the data for an individual
    subject (i.e., one subject at a time) from csv files in a subfolder of the
    "data" directory, creates a pandas dataframe and adds the current subject's
    data to an existing database, specified as DB_NAME, containing other
    subjects' data. If the database does not exist, this script creates it.
