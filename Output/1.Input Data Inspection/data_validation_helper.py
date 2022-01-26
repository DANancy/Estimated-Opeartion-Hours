# Author: Yangyang Cai
# Date: 25/02/2022
# This module is built to help validation job

import glob
import pandas as pd
import os
import re
import numpy as np

def get_file_list(folder_path, file_pattern):
    """
    Get a list to show all files under the folder 
    :param folder_path: folder path for lookup
    :param file_pattern: file pattern for lookup
    :return: list with file paths
    """
    result = [f for f in os.listdir(folder_path) if re.search(file_pattern, f)]
    return result

def get_name_list(folder_path, file_pattern):
    """
    Get a list to show all filenames under the folder 
    :param folder_path: folder path for lookup
    :param file_pattern: file pattern for lookup
    :return: list with filenames
    """
    result = []
    for path in glob.glob(f"{folder_path}\\{file_pattern}"):
        nmi = os.path.basename(path).split(".")[0]
        result.append(nmi)
    return result

def get_file_dict(folder_path, file_pattern):
    """
    Get a dictonary to list all files under the folder 
    :param folder_path: folder path for lookup
    :param file_pattern: file pattern for lookup
    :return: dictionary with file name as key and file path as value
    """
    result = {}
    for path in glob.glob(f"{folder_path}\\{file_pattern}"):
        nmi = os.path.basename(path).split(".")[0]
        result[nmi] = path
    return result

def load_csv(file_path, file_name):
    """
    Load csv to dataframe
    :param file_path: input file path for data loading
    :param file_name: input file name for data loading    
    :return: dataframe 
    """
    try:
        df = pd.read_csv(file_path)
        df.columns.name = file_name
        print(f"{file_path} is valid csv file.")
    except FileNotFoundError:
        print(f"{file_path} is not found.")
    except pd.errors.EmptyDataError:
        print(f"{file_path} have no data.")
    except pd.errors.ParserError:
        print(f"{file_path} has parse error.")
    except Exception:
        print(f"{file_path} is not valid and have some other exceptions.")
    finally:
        return df
    
def check_header(file_path, expected_header):
    """
    Check csv file has header
    :param file_path: input file path for lookup
    :param expected_header: expected header list    
    """
    input_header = pd.read_csv(file_path,nrows=0).columns.tolist()
    if input_header == expected_header:
        print(f"{file_path} has correct csv header as {expected_header}.")
    else:
        print(f"{file_path} does not have correct csv header as {expected_header}.")

def check_columns(df, expected_columns):
    """
    Check columns are formated as expected
    :param df: input df
    :param expected_columns: expected columns    
    """
    if df.columns.to_list() == expected_columns:
        print(f"{df.columns.name} shows corrent data format as {expected_columns}.")
    else:
        print(f"{df.columns.name} does not show corrent data format as {expected_columns}.")

def check_file_format(input_list, expected_format):
    """
    Check file format is as expected
    :param input_lis: input list 
    :param expected_format: expected file format
    """
    if len(input_list) == 0:
        print(f"All items are {expected_format}")
    else:
        print(f"Have {len(input_list)} items are not {expected_format}.")

def check_unique(file_list):
    """
    Check file content is as expected
    :param file_list: input list 
    """
    if len(file_list) == len(set(file_list)):
        print(f"All items are unique.")
    else:
        print(f"Some items are not unqiue.")
    
def check_missing_nmi(input_list, compared_list):
    """
    Check nmi has consumption data but is missing from nmi master file
    :param input_list: all nmis from master file
    :param compared_list: all nmis has consumption data
    """
    missing_nmi = []
    for item in input_list:
        if item not in compared_list:
            missing_nmi.append(item)
    print(f"Each nmi in {missing_nmi} is missing from nmi master file but has consumption data.")
    
def check_missing_data(df):
    """
    Check input dataframe contains missing data or not
    :param df: input dataframe
    """    
    if df.isnull().values.any():
        print(f"{df.columns.name} has missing data.")
    else:
        print(f"{df.columns.name} does not have missing data.")
    
def check_missing_consumption(input_list, compared_list):
    """
    Check nmi from nmi master file is missing consumption data
    :param input_list: all nmis has consumption data
    :param compared_list: all nmis from master file 
    """
    missing_consumption = []
    for item in input_list:
        if item not in compared_list:
            missing_consumption.append(item)
    print(f"Each nmi in {missing_consumption} in nmi master file does not have consumption data.")
    
def check_column_type(df,column_name,expected_type):
    """
    Check data frame column has expected data type
    :param df: input dataframe
    :param column_name: lookup column name
    :param expected_type: expected data type
    """
    if df[column_name].dtype == expected_type:
        print(f"The {column_name} column of {df.columns.name} has correct {expected_type} type")
    else:
        print(f"The {column_name} column of {df.columns.name} has incorrect {df[column_name].dtype } type, should be {expected_type} type.") 
    
def check_value(df, column_name, expected_value_list):
    """
    Check data frame column contain expected values
    :param df: input dataframe
    :param column_name: lookup column name
    :param expected_value_list: expected value list
    """
    input_list = sorted(df[column_name].unique())
    result = [element for element in input_list if element not in expected_value_list]
    if len(result) == 0:
        print(f"The {column_name} in {df.columns.name} has correct value from {expected_value_list}")
    else:
        print(f"The {column_name} in {df.columns.name} has incorrect value as {result} not from {expected_value_list}") 

def check_duplicate_rows(df):
    """
    Check data frame column contain duplicate rows
    :param df: input dataframe
    """
    if len(df[df.duplicated()]) == 0:
        print(f"{df.columns.name} does not have duplicate rows")
    else:
        print(f"{df.columns.name} have duplicate rows")
        
def check_consistent(df,column_name):
    """
    Check data frame column contain consistent values
    :param df: input dataframe
    :param column_name: lookup column name
    """
    input_list = sorted(df[column_name].unique())
    if len(input_list) == 1:
        print(f"The {column_name} in {df.columns.name} has consistent values from {input_list}")
    else:
        print(f"The {column_name} in {df.columns.name} has inconsistent values from {input_list}") 
    
def check_outlier(df, column_name):
    """
    Check data frame column contain outliers
    :param df: input dataframe
    :param column_name: lookup column name
    """    
    Q1 = df[column_name].quantile(0.25)
    Q3 = df[column_name].quantile(0.75)
    IQR = Q3 - Q1
    lower_lim = Q1 - 1.5 * IQR
    upper_lim = Q3 + 1.5 * IQR
    outliers = df[((df[column_name] < lower_lim) |(df[column_name] > upper_lim))]
    if len(outliers) == 0:
        print(f"{df.columns.name} does not have outliers.")
    else:
        print(f"{df.columns.name} has outliers as {len(outliers)}.")
        
def check_datetime_freq(df, date_column):
    """
    Check data frame frequency
    :param df: input dataframe
    :param column_name: lookup column name
    """
    df.dropna()
    df.drop_duplicates(keep=False, inplace=True)
    df.drop_duplicates(subset = [date_column], keep=False, inplace=True)
    try:
        freq = ""
        df['AESTTime'] = pd.to_datetime(df['AESTTime'])
        df = df.set_index('AESTTime')
        diffs = (df.index[1:] - df.index[:-1])
        min_delta = diffs.min()
        mask = (diffs == min_delta)[:-1] & (diffs[:-1] == diffs[1:])
        pos = np.where(mask)[0][0]
        freq = pd.infer_freq(df.index[pos: pos + 3])
        print(f"{df.columns.name} shows consistent inverval as {freq}.")
    except Exception:
        print(f"{df.columns.name} shows inconsistent inverval")