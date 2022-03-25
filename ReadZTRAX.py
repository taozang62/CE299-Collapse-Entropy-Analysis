# -*- coding: utf-8 -*-
"""

Created on Thu Feb 15 16:57:59 2018
@author: wyatt clarke
@source: https://github.com/zillow-research/ztrax

"""

# MACHINERY TO WORK WITH ZTRAX DATA
############################################################################################################
import pandas as pd
import os 
import time

# Zillow Dataset Structure
#     Zillow
#       |
#     State
#   |        |
# Asmt     Trans
#  |         |
# Tables   Tables


# All paths are changed accordingly on personal Macbook. 
# Load layout file for ZTrans
# https://stackoverflow.com/questions/64264563/attributeerror-elementtree-object-has-no-attribute-getiterator-when-trying
layout_ZTrans = pd.read_excel('/Users/taozang/CE299-Collapse-Entropy-Analysis/Layout.xlsx', sheet_name='ZTrans', engine='openpyxl')
layout_ZAsmt = pd.read_excel('/Users/taozang/CE299-Collapse-Entropy-Analysis/Layout.xlsx', sheet_name='ZAsmt', engine='openpyxl')
# LOAD FUNCTIONS FOR READING ZTRAX TABLES
# Function for reading a Ztrans table with row and column criteria
def read_ZTrans(state_code, table_name, col_indices, row_crit_field, row_crit_content, path2zillow):
    path = "{}/{}/ZTrans/{}.txt".format(path2zillow, state_code, table_name) # modify zillow data path here
    layout_temp = layout_ZTrans.loc[layout_ZTrans.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000

    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])

# Function for reading a ZAsmt table with row and column criteria
def read_ZAsmt(state_code, table_name, col_indices, row_crit_field, row_crit_content, path2zillow):
    path = "{}/{}/ZAsmt/{}.txt".format(path2zillow, state_code, table_name)
    layout_temp = layout_ZAsmt.loc[layout_ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000

    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])

# Function for reading a Ztrans table with ALL rows and column criterion
def read_ZTrans_long(state_code, table_name, col_indices, path2zillow):
    path = "{}/{}/ZTrans/{}.txt".format(path2zillow, state_code, table_name)
    layout_temp = layout_ZTrans.loc[layout_ZTrans.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3

    return pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices)

# Function for reading a ZAsmt table with ALL rows and column criterion
def read_ZAsmt_long(state_code, table_name, col_indices, path2zillow):
    path = "{}/{}/ZAsmt/{}.txt".format(path2zillow, state_code, table_name)
    layout_temp = layout_ZAsmt.loc[layout_ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName'][col_indices]
    dtype=layout_temp['PandasDataType'][col_indices].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3

    return pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, usecols=col_indices)

# Function for reading a Ztrans table with row criterion and ALL columns
def read_ZTrans_wide(state_code, table_name, row_crit_field, row_crit_content, path2zillow):
    path = "{}/{}/ZTrans/{}.txt".format(path2zillow, state_code, table_name)
    layout_temp = layout_ZTrans.loc[layout_ZTrans.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName']
    dtype=layout_temp['PandasDataType'].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000

    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])

# Function for reading a ZAsmt table with row criterion and ALL columns
def read_ZAsmt_wide(state_code, table_name, row_crit_field, row_crit_content, path2zillow):
    path = "{}/{}/ZAsmt/{}.txt".format(path2zillow, state_code, table_name)
    layout_temp = layout_ZAsmt.loc[layout_ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName']
    dtype=layout_temp['PandasDataType'].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000
    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])

# Function for reading a ZAsmtHist table with row criterion and ALL columns
def read_ZAsmtHist_wide(state_code, table_name, row_crit_field, row_crit_content, path2zillow):
    path = "{}/{}/ZAsmt/{}.txt".format(path2zillow, state_code, table_name)
    layout_temp = layout_ZAsmt.loc[layout_ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName']
    dtype=layout_temp['PandasDataType'].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    chunksize=500000

    iter = pd.read_csv(path, quoting=quoting, names=names, dtype=dtype, encoding=encoding, sep=sep, header=header, iterator=True, chunksize=chunksize)
    return pd.concat([chunk[(chunk[row_crit_field].isin(row_crit_content))] for chunk in iter])
##########################################################################################################

def tic():
    #Homemade version of matlab tic and toc functions
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()
def toc():
    if 'startTime_for_tictoc' in globals():
        print("Elapsed time is " + str((time.time() - startTime_for_tictoc)) + " seconds, or " + str((time.time() - startTime_for_tictoc)/60) + " minutes.")
    else:
        print("Toc: start time not set")
