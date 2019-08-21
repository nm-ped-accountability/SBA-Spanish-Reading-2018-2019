#Jeanho Rodriguez
# SBA_SPANISH_Fall_2019
#Date: 8/15/2019.

import pprint
import pandas as pd
import json
import xlrd
import numpy as np
import os
#import jinja2
import datetime
os.getcwd()
os.listdir(os.getcwd())
from numpy import nan as NA
from pandas import Series, DataFrame
from functools import reduce
from collections import Counter

# set display
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

# increase column width
pd.set_option('display.max_colwidth', 1000)

df = pd.read_csv("NMSRR1819Admin6StudentResultsUNMASKED.csv", sep=',', header=0, low_memory=False)
df_120 = pd.read_csv("df_STUDENT_120.csv", sep=',', header=0, low_memory=False)
df_program = pd.read_csv("df_Program.csv", sep=',', header=0, low_memory=False)

df.rename(columns={'rptStudID':'STUDENT_ID'}, inplace=True)

df['STUDENT_ID'] = df['STUDENT_ID'].astype(int)
df_120['STUDENT_ID'] = df_120['STUDENT_ID'].astype(int)

merge = [df,df_120]

merge_df = reduce(lambda left,right: pd.merge(left,right,on=['STUDENT_ID'], how='outer'), merge)

merge_df.drop(merge_df[merge_df.StuGrade.isin(['NaN'])].index, inplace=True)

mergeII = [merge_df, df_program]

merge_df_program = reduce(lambda left,right: pd.merge(left,right,on=['STUDENT_KEY'], how='outer'), mergeII)

merge_df_program.drop(merge_df_program[merge_df_program.StuGrade.isin(['NaN'])].index, inplace=True)

mergeII = [merge_df,df_program]

merge_df_program = reduce(lambda left,right: pd.merge(left,right,on=['STUDENT_KEY'], how='outer'), mergeII)

merge_df_program.rename(columns={'STUDENT_ID':'Code'}, inplace=True)

# Load M-Schools V3
schools_df = pd.read_csv('Master Schools 2019 V3.csv', header=0,dtype={'schnumb':float},low_memory=False)

# Choose SY = 2019
schools_df = schools_df.loc[schools_df['SY'] == '2019']

schools_df = schools_df[['schnumb','schname','distname','distcode','level']]


schools_df.rename(columns={'schnumb':'Code'}, inplace=True)

data_frames = [merge_df_program, schools_df]

#merge dataframe to add school names
merged_schools = reduce(lambda left,right: pd.merge(left,right,on=['Code'], how='outer'), data_frames)

merged_schools.drop(merged_schools[merged_schools.StuGrade.isin(['NaN'])].index, inplace=True)

merged_schools['Code'] = merged_schools['Code'].astype(int)

merged_schools['schnumb'] = merged_schools['DisCode'] * 1000 + merged_schools['SchCode']


#merge dataframe to add school names for vendor schools
schools_dfII = schools_df.copy()

schools_dfII.rename(columns={'Code':'schnumb'}, inplace=True)
schools_dfII.rename(columns={'schname':'schnameII'}, inplace=True)
schools_dfII.rename(columns={'distname':'distnameII'}, inplace=True)
schools_dfII.rename(columns={'distcode':'distcodeII'}, inplace=True)
schools_dfII.rename(columns={'level':'levelII'}, inplace=True)


data_framesII = [merged_schools,schools_dfII]

merged_schoolsII = reduce(lambda left,right: pd.merge(left,right,on=['schnumb'], how='outer'), data_framesII)


# Irregularity Tests
irregs = [797415916,172741936,392216412,614677391,559375175,264243924,171377237,797982576,716458690,891345845,784964140,
         287913446,395756158,127815165,416568533,786855288,259941656,673824371,271378887,281568188,296829393,899953962,
         267781318,292712940,382842664,811122449,425649977,648695823,688788249]

# Remove any irregs
merged_schoolsII.loc[merged_schoolsII['Code'].isin(irregs),:] = np.nan

merged_schoolsII['ReadScaleScore'] = merged_schoolsII['ReadScaleScore'].astype(str)

# last two of SS to get ReadScaleScore
merged_schoolsII['SS'] = merged_schoolsII['ReadScaleScore'].str[-4:]


# choose columns
merged_schoolsII = merged_schoolsII[['Code','schnumb','DisCode','distnameII','SchCode','schnameII','LName','FName','MI',
                                     'StuGrade','CURR_GRADE_LVL','ReadPerformanceLevel','SS','BookletID']]

merged_schoolsII['Tested_Grade_Listen'] = str
merged_schoolsII['Tested_Grade_Read'] = str
merged_schoolsII['Tested_Grade_Speak'] = str
merged_schoolsII['Tested_Grade_Write'] = str
merged_schoolsII['Accomm'] = str
merged_schoolsII['CBT'] = 'Y'
merged_schoolsII['CBT_Listen'] = str
merged_schoolsII['CBT_Read'] = str
merged_schoolsII['CBT_Speak'] = str
merged_schoolsII['CBT_Write'] = str
merged_schoolsII['Testname'] = str
merged_schoolsII['Subtest'] = 'FALL_SPANISH'
merged_schoolsII['TestCode'] = 'SPAN_FALL'
merged_schoolsII['TestLang'] = 'S'
merged_schoolsII['PL_Listen'] = str
merged_schoolsII['PL_Read'] = str
merged_schoolsII['PL_Speak'] = str
merged_schoolsII['PL_Write'] = str
merged_schoolsII['PL_Comprehension'] = str
merged_schoolsII['PL_Oral'] = str
merged_schoolsII['PL_Literacy'] = str
merged_schoolsII['Proficient'] = 'N'
merged_schoolsII['NewSS'] = str
merged_schoolsII['SS_Listen'] = str
merged_schoolsII['SS_Read'] = str
merged_schoolsII['SS_Speak'] = str
merged_schoolsII['SS_Write'] = str
merged_schoolsII['SS_Comprehension'] = str
merged_schoolsII['SS_Oral'] = str
merged_schoolsII['SS_Literacy'] = str
merged_schoolsII['IstationTime'] = str
merged_schoolsII['Pearson_SGP'] = str

# Rename Variables
merged_schoolsII.rename(columns={'Code':'StID'}, inplace=True)
merged_schoolsII.rename(columns={'BookletID':'TestbookID'}, inplace=True)
merged_schoolsII.rename(columns={'schnumb':'Vendor_SchNumb'}, inplace=True)
merged_schoolsII.rename(columns={'DisCode':'Vendor_DistCode'}, inplace=True)
merged_schoolsII.rename(columns={'distnameII':'Vendor_DistName'}, inplace=True)
merged_schoolsII.rename(columns={'SchCode':'Vendor_SchCode'}, inplace=True)
merged_schoolsII.rename(columns={'schnameII':'Vendor_SchName'}, inplace=True)
merged_schoolsII.rename(columns={'LName':'Last'}, inplace=True)
merged_schoolsII.rename(columns={'FName':'First'}, inplace=True)
merged_schoolsII.rename(columns={'MI':'MI'}, inplace=True)
merged_schoolsII.rename(columns={'StuGrade':'Tested_Grade'}, inplace=True)
merged_schoolsII.rename(columns={'ReadPerformanceLevel':'PL'}, inplace=True)


# drop NaN
merged_schoolsII.drop(merged_schoolsII[merged_schoolsII.PL.isin(['nan'])].index, inplace=True)

# Compute Grade and PL (no PL = 4)
merged_schoolsII['PL'] = merged_schoolsII['PL'].astype(int)
merged_schoolsII['Pref_Grade'] = merged_schoolsII['CURR_GRADE_LVL']
merged_schoolsII['STARS_Grade'] = merged_schoolsII['CURR_GRADE_LVL']
merged_schoolsII.loc[(merged_schoolsII['PL'] == 3), 'Proficient'] = 'Y'

# Undupe StID
merged_schoolsII= merged_schoolsII.groupby(['StID']).first().reset_index()

# Order and Select Columns by index
merged_schoolsII = merged_schoolsII.iloc[:, np.r_[13,0,1,2,3,4,5,6,7,8,9,14,15,16,17,46,47,18,19,20,21,22,23,24,25,26,27
                                                 ,11,28,29,30,31,32,33,34,35,12,36,37,38,39,40,41,42,43,44,45]]


Subtest_map = {'FALL_SPANISH':'SPANISH'}

merged_schoolsII['Subtest'] = merged_schoolsII['Subtest'].map(Subtest_map) #map function applied with map rule

merged_schoolsII['Tested_Grade'] = merged_schoolsII['Tested_Grade'].astype(int)

merged_schoolsII['Tested_Grade'] = merged_schoolsII['Tested_Grade'].astype(str)

merged_schoolsII['TestCode'] = 'SPA' + merged_schoolsII['Tested_Grade']

print(merged_schoolsII['Subtest'].value_counts(dropna=False))


#merged_schoolsII.to_csv("SBA SPANISH FALL for 2019 DAD 2019-08-12V2.csv", sep=',', encoding='utf-8-sig', index = False)




