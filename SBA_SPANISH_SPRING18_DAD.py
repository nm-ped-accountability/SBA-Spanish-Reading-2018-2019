#Jeanho Rodriguez.
# SBA_SPANISH_2019_SPRING
#Date: 6/28/2019.

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

df = pd.read_csv("SBA_SPAN_merged_schools.csv", sep=',', header=0, low_memory=False)

df['Vendor_SchNumb'] = (df['DisCode'] * 1000) + df['SchCode']

df['STARS_SchNumb'] = (df['S_DISTRICT_CODE'] * 1000) + df['S_LOCATION_CODE']
df['Proficient'] = 'N'

df.loc[(df['AccYN'] == 1.0), 'AccYN'] = 'Y'
df.loc[(df['AccYN'] == 0.0), 'AccYN'] = 'N'

df.loc[(df['CBT'] == 'Computer Based Test'), 'CBT'] = 'Y'
df.loc[(df['TestName'] == 'SBASPAN'), 'TestName'] = 'SBA Spanish'

# Recode Grade == 'HS'
df.loc[df['Grade'] == 'HS', 'Grade'] = df['StuGrade']


df['NewSS'] = int
df['SSRead'] = int
df['SSWrite'] = int
df['IstationTime'] = int
df['TestCode'] = 'SPAN'

# Match vendor_schnum to masterschools
schools_df = pd.read_csv('Master Schools 2019 V3.csv', header=0,dtype={'schnumb':float},low_memory=False)

# Choose SY = 2019
schools_df = schools_df.loc[schools_df['SY'] == '2019']

schools_df = schools_df[['schnumb','schname','distname']]

schools_df.rename(columns={'schnumb':'Vendor_SchNumb'}, inplace=True)

data_frames = [df, schools_df]

#merge dataframe to add school names
merged_schools = reduce(lambda left,right: pd.merge(left,right,on=['Vendor_SchNumb'], how='outer'), data_frames)


# drop NaN
merged_schools.drop(merged_schools[merged_schools.PL.isin(['NaN'])].index, inplace=True)

merged_schools['PL'] = merged_schools['PL'].astype(int)

merged_schools.loc[(merged_schools['PL']== 3), 'Proficient'] = 'Y'
merged_schools.loc[(merged_schools['PL']== 4), 'Proficient'] = 'Y'


# rename columns
merged_schools.rename(columns={'STUID':'StID'}, inplace=True)
merged_schools.rename(columns={'S_DISTRICT_CODE':'STARS_DistCode'}, inplace=True)
merged_schools.rename(columns={'S_LOCATION_CODE':'STARS_SchCode'}, inplace=True)
merged_schools.rename(columns={'LName':'Last'}, inplace=True)
merged_schools.rename(columns={'FName':'First'}, inplace=True)
merged_schools.rename(columns={'StuGrade':'Tested_Grade'}, inplace=True)
merged_schools.rename(columns={'S_GRADE':'STARS_Grade'}, inplace=True)
merged_schools.rename(columns={'AccYN':'Accomm'}, inplace=True)
merged_schools.rename(columns={'schname_y':'Vendor_SchName'}, inplace=True)
merged_schools.rename(columns={'distname_y':'Vendor_DistName'}, inplace=True)
merged_schools.rename(columns={'SchCode':'Vendor_SchCode'}, inplace=True)
merged_schools.rename(columns={'DisCode':'Vendor_DistCode'}, inplace=True)


# pref_grade
merged_schools['Pref_Grade'] = merged_schools['STARS_Grade']


# add additional variables
merged_schools['Tested_Grade_Listen'] = str
merged_schools['Tested_Grade_Read'] = str
merged_schools['Tested_Grade_Speak'] = str
merged_schools['Tested_Grade_Write'] = str
merged_schools['CBT_Listen'] = str
merged_schools['CBT_Read'] = str
merged_schools['CBT_Speak'] = str
merged_schools['CBT_Write'] = str
merged_schools['PL_Listen'] = str
merged_schools['PL_Read'] = str
merged_schools['PL_Speak'] = str
merged_schools['PL_Write'] = str
merged_schools['PL_Comprehension'] = str
merged_schools['PL_Oral'] = str
merged_schools['PL_Literacy'] = str
merged_schools['SS_Listen'] = str
merged_schools['SS_Read'] = str
merged_schools['SS_Speak'] = str
merged_schools['SS_Write'] = str
merged_schools['SS_Comprehension'] = str
merged_schools['SS_Oral'] = str
merged_schools['SS_Literacy'] = str
merged_schools['Pearson_SGP'] = str


merged_schools = merged_schools[['TestbookID','StID','Vendor_SchNumb','Vendor_DistCode','Vendor_DistName','Vendor_SchCode',
                                 'Vendor_SchName','Last','First','MI','Tested_Grade','Tested_Grade_Listen','Tested_Grade_Read',
                                 'Tested_Grade_Speak','Tested_Grade_Write','Pref_Grade','STARS_Grade','Accomm','CBT','CBT_Listen',
                                 'CBT_Read','CBT_Speak','CBT_Write','TestName','Subtest','TestCode','Testlang','PL','PL_Listen',
                                 'PL_Read','PL_Speak','PL_Write','PL_Comprehension','PL_Oral','PL_Literacy','Proficient','SS',
                                 'NewSS','SS_Listen','SS_Read','SS_Speak','SS_Write','SS_Comprehension','SS_Oral','SS_Literacy',
                                 'IstationTime','Pearson_SGP']]


merged_schools['Subtest'] = 'SPANISH'

merged_schools['Tested_Grade'] = merged_schools['Tested_Grade'].astype(int)

merged_schools['Tested_Grade'] = merged_schools['Tested_Grade'].astype(str)

Tested_Grade_map = {'3':'03','4':'04','11':'11','5':'05','6':'06','7':'07','8':'08','10':'10'}

merged_schools['Tested_Grade'] = merged_schools['Tested_Grade'].map(Tested_Grade_map) #map function applied with map rule

merged_schools['TestCode'] = 'SPA' + merged_schools['Tested_Grade']


merged_schools.to_csv("SBA SPANISH SPRING for 2019 DAD 2019-08-12V3.csv", sep=',', encoding='utf-8-sig', index = False)













