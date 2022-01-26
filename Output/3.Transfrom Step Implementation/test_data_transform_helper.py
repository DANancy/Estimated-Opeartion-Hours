# Author: Yangyang Cai
# Date: 25/01/2022
# This module is built to do unit test for data transformation job

import data_transform_helper as dth
import unittest
import pandas as pd

class TransformNMITest(unittest.TestCase):
    
    def setUp(self):
        print("Tranform NMI Test Data Setup Called...")
        self.nmi_file = '..\\1.Input Data Inspection\\Data\\nmi_info.csv'  
        self.output_path = 'Transformed\\'
        self.nmi_df = dth.transform_nmi_master(self.nmi_file, self.output_path)
        
    def test_0_column_name_nmi(self):
        """
        Test columns name are all uppercase 
        """ 
        actual = self.nmi_df.columns.to_list()
        expected = ['NMI','STATE','INTERVAL']
        self.assertEqual(actual, expected)
    
    def test_1_column_type_nmi(self):
        """
        Test columns type meet requirements
        """          
        actual = self.nmi_df.dtypes.to_list()
        expected = ['O', 'O', 'int32']
        self.assertEqual(actual, expected)
    
    def test_2_standardized_state_values_nmi(self):
        """
        Test column values are all uppercase
        """        
        actual = [column for column in self.nmi_df['STATE'].unique().tolist() if column.islower()]
        expected = []
        self.assertEqual(actual, expected)
    
    def tets_3_duplication_nmi(self):
        """
        Test no duplications
        """  
        actual = self.nmi_df.duplicated().unique().tolist()[0]
        expected = False
        self.assertEqual(actual, expected)
    
    def test_4_missing_data_nmi(self):
        """
        Test no missing data 
        """  
        actual = self.nmi_df.isna().any().any()
        expected = False
        self.assertEqual(actual, expected)

class TransformConsumptionTest(unittest.TestCase):
    def setUp(self):
        print("Tranform Consumption Test Data Setup Called...")
        self.consumption_folder = '..\\1.Input Data Inspection\\Data\\ConsumptionData'
        self.consumption_df = dth.transform_consumption(self.consumption_folder)
        
    def test_5_consistent_unit_values_consumption(self):
        """
        Test column values are unique as KWH
        """
        actual = self.consumption_df['UNIT'].unique().tolist()[0]
        expected = 'KWH'
        self.assertEqual(actual, expected)   
    
    def tets_6_duplication_consumption(self):
        """
        Test no duplications
        """  
        actual = self.consumption_df.duplicated().unique().tolist()[0]
        expected = False
        self.assertEqual(actual, expected)
    
    def test_7_missing_data_datetime_consumption(self):
        """
        Test no missing data 
        """
        actual = self.consumption_df.isna().any().any()
        expected = False
        self.assertEqual(actual, expected)
    
    def test_8_local_time_consumption(self):
        """
        Test AEST time has transformed to be local time
        """
        default_timezone = 'Australia/Brisbane'
        transform_dict = {'VIC': 'Australia/Victoria',
                          'NSW': 'Australia/NSW',
                          'SA': 'Australia/Adelaide',
                           'WA': 'Australia/West',
                          'TAS':'Australia/Tasmania',
                           'QLD': 'Australia/Queensland',
                           'ACT': 'Australia/ACT'}
        
        test_df = self.consumption_df[self.consumption_df['STATE'] == 'NSW'][['TRANSFORMED_AESTTIME', 'AESTTIME']]
        test_df['ACTUAL'] = test_df['AESTTIME'].dt.tz_localize(default_timezone).dt.tz_convert(transform_dict['NSW'])
        test_df = test_df.set_index(test_df['ACTUAL'])
        test_df['ACTUAL'] = test_df.index.strftime('%Y-%m-%d %H:%M:%S')
        test_df = test_df.astype({'ACTUAL':'datetime64[ns]'}).set_index('ACTUAL')
        test_df.reset_index(drop=False, inplace=True)
        result = test_df['TRANSFORMED_AESTTIME'] == test_df['ACTUAL']
        
        actual = list(result.unique())[0]
        expected = True
        self.assertEqual(actual, expected)
        
if __name__ == '__main__':
    unittest.main()