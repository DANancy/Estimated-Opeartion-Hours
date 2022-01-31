# Estimated-Opeartion-Hours

### Project Structure
* Output
    * **1.Input Data Inspection**
    * 1.1 Data Validation and Data Verification Report.docx
    * 1.2 data_validation_helper.py
    * 1.3 data_validatoin_and_data_verification.ipynb
    * **2. Process Map**
    * 2.1 Process_Flow_and_Diagrams.pptx
    * 2.2 Shell Energy Dashboard ELT - Simple Solution.png
    * 2.3 Shell Energy Dashboard ETL - Cloud Solution.png
    * **3. Transform Step Implementation**
    * 3.1 data_transform_helper.py
    * 3.2 data_transformation.ipynb
    * 3.3 data_validation_helper.py
    * 3.4 database_helper.py
    * 3.5 test_data_transform_helper.py
    * **4. Analysis**
    * 4.1 NMI_Hourly_Consumption_Report.csv
    * 4.2 Estimated_Operation_Hours_Dashboard.pbix
    * 4.3 Estimated_Operation_Hours_Dashboard.jpg
    
* test_data_transform_helper.py: unittest file contains 8 test cases
    * test_0_column_name_nmi
    * test_1_column_type_nmi
    * test_2_standardized_state_values_nmi
    * tets_3_duplication_nmi
    * test_4_missing_data_nmi
    * test_5_consistent_unit_values_consumption
    * tets_6_duplication_consumption
    * test_7_missing_data_datetime_consumption
    * test_8_local_time_consumption

### Project Setup
1. clone the whole project
```
$ git clone https://github.com/DANancy/Estimated-Operation-Hours.git
```

2. add data folder under 1.Input Data Inspection

3. go to transformation folder
```
$ cd Output/3.Transfrom Step Implementation
```

4. run transform, open data_transformation.ipynb and run all cells
```
$ python -m notebook
```

5. run unittest
```
$ python -m unittest -v test_data_transform_helper.py
```

6.  run test coverage report to gauge the effectiveness of tests
```
$ pip install coverage
$ coverage run -m unittest
$ coverage html
$ open htmlcov/index.html
```
