import pandas as pd
import time
from math import log
import collections
def tic():
    #Homemade version of matlab tic and toc functions
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()
def toc():
    if 'startTime_for_tictoc' in globals():
        print("Elapsed time is " + str((time.time() - startTime_for_tictoc)) + " seconds, or " + str((time.time() - startTime_for_tictoc)/60) + " minutes.")
    else:
        print("Toc: start time not set")


# Function removeID (dataframe):
#     return the index of dataframe without ID columns
#     and the associated ID Columns.
# If the dataframe is none, return -1
def removeID (df):
    if df.empty: return -1
    colmn = df.columns
    ID_list = []
    for col in colmn:
        if col.find('ID') != -1:
            ID_list.append(col)
    return colmn.drop(ID_list)

def get_entropy_helper(new_list):
    return -sum([ p * log(p) for p in new_list])

# Function get_entropy(dataframe, column_name) takes the 
# dataframe from pandas and the targeted column inside the dataframe 
# and calculate the distribution of pdf and the entropy value. 
def get_entropy(df, col_name):
    dtype = df[col_name].dtypes
    dt = df[col_name].dropna().tolist()
    counter = collections.Counter(dt)
    val = counter.values()
    new_list = [x / len(dt) for x in val]
    entropy = get_entropy_helper(new_list)
    if sum(new_list) - 1 <= 5e-8:
        return col_name, entropy
    return -1


def entropy_analysis_single_table(table, feature_list, entropy_list):
    tmp = pd.read_csv('{}.csv'.format(table), low_memory=False) 
    if tmp.empty: return
    cols_no_id = removeID(tmp)
    for col in cols_no_id:
        feature, entropy = get_entropy(tmp, col)
        feature_list.append(feature)
        entropy_list.append(entropy)
    return    

def entropy_analysis_multiple_tables(table_list):
    feature_list = []
    entropy_list = []
    for table in table_list:
        tic()
        # update the feature and entropy list
        entropy_analysis_single_table(table, feature_list, entropy_list)
        toc()
        print('The analysis for ' + table + ' is done!')
    return feature_list, entropy_list