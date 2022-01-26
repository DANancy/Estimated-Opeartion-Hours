# Author: Yangyang Cai
# Date: 25/02/2022
# This module is built to help transform job

import pandas as pd
import csv
import os
import data_validation_helper as dvh
import numpy as np

def _transform_to_csv(file_path):
    detected_delimiter = ','
    with open(file_path, newline='') as f:
        reader = csv.reader(f)
        header = next(reader) 
        row = next(f)
        
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(row)
        detected_delimiter = dialect.delimiter
    
    file_name = os.path.basename(file_path).split(".")[0]
    df = dvh.load_csv(file_path, delimiter = detected_delimiter)
    df.to_csv(f'{file_name}.csv', index = None)

def transform_nmi_master(file_path, output_folder):
    """
    Transform nmi master data based on requirement
    :param file path: input file path (must be .csv)
    :param output folder: output folder to savae transformed files 
    :return: transformed dataframe and transformed csv file
    """
    file_name = os.path.basename(file_path).split(".")[0]
    df = pd.read_csv(file_path)
    
    # 1.3 Columns names need to be standardized (i.e., all uppercase)
    df.columns = [column.upper() for column in df.columns]
    
    # 1.4 Column type needs to be standardized (i.e., INTERVAL as int)
    df = df.astype({"NMI": str, "STATE": str, "INTERVAL": int})
    
    # 1.5 Columns values need to be standardized (i.e., all uppercase)
    idx = (df.applymap(type) == str).all(0)
    str_list = df[df.columns[idx]].columns.to_list()
    for column in str_list:
        df[column] = df[column].str.upper()
    
    # 1.6 Make sure no duplicate rows
    df.drop_duplicates(keep=False, inplace=True)
    df.drop_duplicates(subset = ['NMI'], keep=False, inplace=True)
   
    # 1.7 Remove rows have missing NMI or STATE
    df = df.dropna(subset=['NMI', 'STATE'])
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    df.to_csv(output_folder + f'transformed_{file_name}.csv', index=None)
    return df


def transform_consumption(folder_path, file_pattern = "*.csv", output_folder='Transformed\\ConsumptionData\\', lookup_file='Transformed\\transformed_nmi_info.csv'):
    """
    Transform consumption data based on requirement
    :param folder_path: folder path for lookup (must be .csv files under folder path)
    :param file_pattern: file pattern for lookup
    :param output folder: output folder to savae transformed files
    :return: transformed merged dataframe and transformed csv file for each input csv file
    """
    consumption_dict = dvh.get_file_dict(folder_path, file_pattern)
    merged_df = pd.DataFrame()
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for name, path in consumption_dict.items():
        if _lookup_nmi(name, lookup_file):
                # 2.2 Column type needs to be standardized (i.e., AESTIME same date format)
                df = pd.read_csv(path, parse_dates=[0])
                df.columns.name = name
                
                # 2.1 Column names need to be standardized (i.e., all uppercase)
                df.columns = [column.upper() for column in df.columns]
                
                # 2.3 Columns values need to be standardized (i.e., UNIT all uppercase)
                idx = (df.applymap(type) == str).all(0)
                str_list = df[df.columns[idx]].columns.to_list()
                for column in str_list:
                    df[column] = df[column].str.upper()
                
                # 2.4 Make sure no duplicate rows
                df.drop_duplicates(keep=False, inplace=True)
                df.drop_duplicates(subset = ['AESTTIME'], keep=False, inplace=True)
                
                # 2.5 Missing data imputation
                df = _missing_data_imputation(df)
                
                # 2.6 Unit measurement needs to be standardized format (i.e., all KWH)
                df['QUANTITY'] = np.where(df['UNIT'] == 'MWH', df['QUANTITY'] * 1000.00, df['QUANTITY'])
                df['QUANTITY'] = np.where(df['UNIT'] == 'WH', df['QUANTITY'] / 1000.00, df['QUANTITY'])
                df['UNIT'] = np.where(df['UNIT'] == 'MWH', 'KWH', 'KWH')
                df['UNIT'] = np.where(df['UNIT'] == 'WH', 'KWH', 'KWH')     
                
                # 2.7 Transform the datetime column to be local time
                df = _transform_to_local_time(df, 'AESTTIME', name, lookup_file)
                
                # 2.8 Add more date features
                df = _get_date_features(df, 'TRANSFORMED_AESTTIME')
                
                # 2.9 Add NMI column when load consumption data
                df['NMI'] = name
                
                # 2.10 Mark outlier before further analysis
                Q1 = df['QUANTITY'].quantile(0.25)
                Q3 = df['QUANTITY'].quantile(0.75)
                IQR = Q3 - Q1
                lower_lim = Q1 - 1.5 * IQR
                upper_lim = Q3 + 1.5 * IQR
                df['OUTLIER'] = np.where((df['QUANTITY'] < lower_lim) | (df['QUANTITY'] > upper_lim),1,0)
                
                df.to_csv(output_folder + f'transformed_{name}.csv', index=None)
                merged_df = merged_df.append(df)
    
    merged_df.to_csv(output_folder + f'transformed_consumption_data_merged.csv', index=None)
    return merged_df

def _missing_data_imputation(df):
    """
    Apply different strategies to deal with missing data
    :param df: input dateframe
    :return: transformed dataframe 
    """
    nan_columns = df.columns[df.isnull().any()].tolist()
    for column in nan_columns:
        if df[column].dtype in ['float64','int']:
            df = df.fillna(method='bfill')
        else:
            df = df.dropna()
    return df

def _get_date_features(df, date_column):
    """
    Define multiple date features based on analytics requirement
    :param df: input dateframe
    :param date_column: datatime column for lookup
    :return: transformed dataframe
    """
    df = df.set_index(df[date_column])
    df[date_column] = df.index.strftime('%Y-%m-%d %H:%M:%S')

    df = df.astype({date_column:'datetime64[ns]'}).set_index(date_column)
    df['DATE'] = df.index.strftime('%Y-%m-%d')
    df['YEAR'] = df.index.year
    df['YEARDAY'] = df.index.dayofyear
    df['MONTH'] = df.index.month
    df['MONTHNAME'] = df.index.month_name()
    df['WEEK'] = df.index.isocalendar().week
    df['DAY'] = df.index.day
    df['DAYNAME'] = df.index.day_name()
    df['HOUR'] = df.index.hour
    df['MINUTE'] = df.index.minute
    df['WEEKDAY'] = df.index.dayofweek
    df['WEEKEND'] = df['WEEKDAY'].apply(lambda x: 1 if x >= 5 else 0)
    df['TIME'] = df.index.time
    df['MONTHDAY'] = df.index.strftime('%m-%d')
    df['HOURMINUTE'] = df.index.strftime('%H:%M')

    bins = [0, 4, 8, 12, 16, 20, 24]
    #labels = ['Late Night', 'Early Morning','Morning','Noon','Eve','Night']
    labels = [1, 2, 3, 4, 5, 6]
    df['SESSION'] = pd.cut(df['HOUR'], bins=bins, labels=labels, include_lowest=True)
    df['SEASON'] = df.apply(_get_season, axis = 1)
    df.reset_index(drop=False, inplace=True)
    
    return df
    
def _get_season(df):
    """
    Define season based on month 
    :param df: input dateframe
    :return: season label
    """
    if df['MONTH'] == 3 | df['MONTH'] == 4 | df['MONTH'] == 5:
        return 1 #'Spring'
    elif df['MONTH'] == 6 | df['MONTH'] == 7 | df['MONTH'] == 8:
        return 2 #'Summer'
    elif df['MONTH'] == 9 | df['MONTH'] == 10 | df['MONTH'] == 11:
        return 3 #'Autumn'
    else:
        return 4 #'Winter' 
           
def _transform_to_local_time(df, date_column, lookup_nmi, lookup_file):
    """
    Transform AEST Datetime to be local time based on state in the master file
    :param df: input dateframe
    :param date_column: datatime column for lookup
    :param lookup_nmi: lookup nmi 
    :param lookup_file: the look up file to get state info
    :return: tranformed dataframe
    """
    df[date_column] = pd.to_datetime(df[date_column])
    lookup_df = pd.read_csv(lookup_file)
    state = lookup_df[lookup_df['NMI'] == lookup_nmi]['STATE'].to_list()[0]
    
    default_timezone = 'Australia/Brisbane'
    transform_dict = {'VIC': 'Australia/Victoria',
                      'NSW': 'Australia/NSW',
                      'SA': 'Australia/Adelaide',
                      'WA': 'Australia/West',
                      'TAS':'Australia/Tasmania',
                      'QLD': 'Australia/Queensland',
                      'ACT': 'Australia/ACT'}
    
    df['STATE'] = state
    transformed_column = f'TRANSFORMED_{date_column}'
    df[transformed_column] = df[date_column].dt.tz_localize(default_timezone).dt.tz_convert(transform_dict[state])
    return df

def _lookup_nmi(lookup_nmi, lookup_file):
    """
    Check nmi in the master file or not
    :param lookup_nmi: lookup nmi
    :param lookup_file: the look up file to get nmi info
    :return: tranformed dataframe
    """
    df = pd.read_csv(lookup_file)
    lookup_list = df['NMI'].values.tolist()
    if lookup_nmi in lookup_list:
        return True
    else:
        return False
    
def _combine_load_data(file_dict):
    """
    Combine and load files into single dataframe
    :param file_dict: file dictory for combine and load
    :param name: column to add
    :return: dataframe 
    """
    df = pd.DataFrame()
    for key, value in file_dict.items():
        data = pd.read_csv(value,parse_dates=[0])
        df = df.append(data)
    return df