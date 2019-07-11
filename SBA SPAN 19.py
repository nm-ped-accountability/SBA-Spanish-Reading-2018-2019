#SBA SPAN - 2019.
#Jeanho Rodriguez.
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

df = pd.read_csv("df_sba_span_prep.csv", sep=',', header=0, low_memory=False)
# change Code to float type
df = df.astype({'Code':float})

# Load M-Schools V3
schools_df = pd.read_csv('Master Schools 2019 V3.csv', header=0,dtype={'schnumb':float},low_memory=False)

# Choose SY = 2019
schools_df = schools_df.loc[schools_df['SY'] == '2019']

schools_df = schools_df[['schnumb','schname','distname']]

schools_df.rename(columns={'schnumb':'Code'}, inplace=True)

data_frames = [df, schools_df]

#merge dataframe to add school names
merged_schools = reduce(lambda left,right: pd.merge(left,right,on=['Code'], how='outer'), data_frames)

# drop NaNs in Subtest Column
merged_schools = merged_schools.dropna(subset=['LName'])

# use master schools names
merged_schools['District'] = merged_schools['distname']
merged_schools['School'] = merged_schools['schname']


# School Level
# make sub dataframe
df_copy_school = merged_schools[['Code','District','School','PL']].copy()
df_copy_school = (df_copy_school.groupby(['Code','District','School','PL'])['PL'].count().reset_index(name='N_PL')) #reset index moves index to columns
df_copy_school= df_copy_school.pivot_table(index=['Code','District','School'] , columns='PL', values='N_PL',
                    aggfunc ='sum', margins=True, dropna=True, fill_value=0)
df_copy_school_index = df_copy_school.reset_index()

df_multi_school_final = df_copy_school.div( df_copy_school.iloc[:,-1], axis =0 ).reset_index()
df_multi_school_final['N'] = df_copy_school_index.All

# District Level
df_copy_district = merged_schools[['DisCode','District','PL']].copy()
df_copy_district['School'] = 'Districtwide'
df_copy_district['Code'] = df_copy_district['DisCode'] * 1000
df_copy_district = (df_copy_district.groupby(['Code','District','School','PL'])['PL'].count().reset_index(name='N_PL')) #reset index moves index to columns
df_copy_district= df_copy_district.pivot_table(index=['Code','District','School'] , columns='PL', values='N_PL',
                    aggfunc ='sum', margins=True, dropna=True, fill_value=0)
df_copy_district_index = df_copy_district.reset_index()
df_multi_district_final = df_copy_district.div( df_copy_district.iloc[:,-1], axis =0 ).reset_index()
df_multi_district_final['N'] = df_copy_district_index.All

# State Level
df_copy_state = merged_schools[['DisCode','District','PL']].copy()
df_copy_state['School'] = 'All Students'
df_copy_state['Code'] = 999999
df_copy_state['District'] = 'Statewide'
df_copy_state = (df_copy_state.groupby(['Code','District','School','PL'])['PL'].count().reset_index(name='N_PL')) #reset index moves index to columns
df_copy_state= df_copy_state.pivot_table(index=['Code','District','School'] , columns='PL', values='N_PL',
                    aggfunc ='sum', margins=True, dropna=True, fill_value=0)
df_copy_state_index = df_copy_state.reset_index()
df_multi_state_final = df_copy_state.div( df_copy_state.iloc[:,-1], axis =0 ).reset_index()
df_multi_state_final['N'] = df_copy_state_index.All

# stack data frames
MERGED_SBA_SPAN= pd.concat([df_multi_school_final,df_multi_district_final,df_multi_state_final], axis =0)

# Rename PL columns
MERGED_SBA_SPAN.rename(columns={1.0:'Level 1'}, inplace=True)
MERGED_SBA_SPAN.rename(columns={2.0:'Level 2'}, inplace=True)
MERGED_SBA_SPAN.rename(columns={3.0:'Level 3'}, inplace=True)
MERGED_SBA_SPAN.rename(columns={4.0:'Level 4'}, inplace=True)

# sort columns by sortcode ascending.
MERGED_SBA_SPAN = MERGED_SBA_SPAN.sort_values(['Code','District','School'], ascending=[True,True,True])


# clean Code... adds leading zeros
MERGED_SBA_SPAN.drop(MERGED_SBA_SPAN[MERGED_SBA_SPAN.Code == 'All'].index, inplace=True)
MERGED_SBA_SPAN = MERGED_SBA_SPAN.astype({'Code':int})
MERGED_SBA_SPAN['Code'] = MERGED_SBA_SPAN['Code'].apply(lambda x: '{0:0>6}'.format(x))

# convert to percentage
MERGED_SBA_SPAN['Level 1'] = MERGED_SBA_SPAN['Level 1'] * 100
MERGED_SBA_SPAN['Level 2'] = MERGED_SBA_SPAN['Level 2'] * 100
MERGED_SBA_SPAN['Level 3'] = MERGED_SBA_SPAN['Level 3'] * 100
MERGED_SBA_SPAN['Level 4'] = MERGED_SBA_SPAN['Level 4'] * 100

# Format str
MERGED_SBA_SPAN['District'] = MERGED_SBA_SPAN['District'].str.title()
MERGED_SBA_SPAN['School'] = MERGED_SBA_SPAN['School'].str.title()

# Drop Unnecessary Columns
MERGED_SBA_SPAN = MERGED_SBA_SPAN.drop(MERGED_SBA_SPAN.columns[[7]], axis=1)


####### MASKING ######################################################################################################
# round to nearest integer
MERGED_SBA_SPAN['Level 1'] = MERGED_SBA_SPAN['Level 1'].round(0)
MERGED_SBA_SPAN['Level 2'] = MERGED_SBA_SPAN['Level 2'].round(0)
MERGED_SBA_SPAN['Level 3'] = MERGED_SBA_SPAN['Level 3'].round(0)
MERGED_SBA_SPAN['Level 4'] = MERGED_SBA_SPAN['Level 4'].round(0)

# remove records with fewer than 10 students
WEB_SBA_SPAN = MERGED_SBA_SPAN
#print(WEB_SBA_SPAN['N'].sum()) # 9503

WEB_SBA_SPAN = MERGED_SBA_SPAN.loc[MERGED_SBA_SPAN['N'] >= 10]
#print(WEB_SBA_SPAN['N'].sum()) # 8866


def masking_rule(x, y):
    # N = 301 or higher
    if x >= 99.0 and y > 300:
        return '≥99'
    elif x <= 1.0 and y > 300:
        return '≤1'
    elif x < 99.0 and y > 300:
        return str(x)

    # N = 201 - 300
    elif x >= 98.0 and 200 < y <= 300:
        return '≥98'
    elif x <= 2.0 and 200 < y <= 300:
        return '≤2'
    elif 2.0 < x < 98.0 and 200 < y <= 300:
        return str(x)

    # N = 101 - 200
    elif x < 3.0 and 100 < y <= 200:
        return '≤2'
    elif 3.0 <= x < 5.0 and 100 < y <= 200:
        return '3-4'
    elif 5.0 <= x < 10.0 and 100 < y <= 200:
        return '5-9'
    elif 10.0 <= x < 15.0 and 100 < y <= 200:
        return '10-14'
    elif 15.0 <= x < 20.0 and 100 < y <= 200:
        return '15-19'
    elif 20.0 <= x < 25.0 and 100 < y <= 200:
        return '20-24'
    elif 25.0 <= x < 30.0 and 100 < y <= 200:
        return '25-29'
    elif 30.0 <= x < 35.0 and 100 < y <= 200:
        return '30-34'
    elif 35.0 <= x < 40.0 and 100 < y <= 200:
        return '35-39'
    elif 40.0 <= x < 45.0 and 100 < y <= 200:
        return '40-44'
    elif 45.0 <= x < 50.0 and 100 < y <= 200:
        return '45-49'
    elif 50.0 <= x < 55.0 and 100 < y <= 200:
        return '50-54'
    elif 55.0 <= x < 60.0 and 100 < y <= 200:
        return '55-59'
    elif 60.0 <= x < 65.0 and 100 < y <= 200:
        return '60-64'
    elif 65.0 <= x < 70.0 and 100 < y <= 200:
        return '65-69'
    elif 70.0 <= x < 75.0 and 100 < y <= 200:
        return '70-74'
    elif 75.0 <= x < 80.0 and 100 < y <= 200:
        return '75-79'
    elif 80.0 <= x < 85.0 and 100 < y <= 200:
        return '80-84'
    elif 85.0 <= x < 90.0 and 100 < y <= 200:
        return '85-89'
    elif 90.0 <= x < 95.0 and 100 < y <= 200:
        return '90-94'
    elif 95.0 <= x < 98.0 and 100 < y <= 200:
        return '95-97'
    elif 98.0 <= x and 100 < y <= 200:
        return 'GE 98'

    # N = 41 - 100
    elif 6.0 > x and 40 < y <= 100:
        return '≤5'
    elif 6.0 <= x < 10.0 and 40 < y <= 100:
        return '6-9'
    elif 10.0 <= x < 15.0 and 40 < y <= 100:
        return '10-14'
    elif 15.0 <= x < 20.0 and 40 < y <= 100:
        return '15-19'
    elif 20.0 <= x < 25.0 and 40 < y <= 100:
        return '20-24'
    elif 25.0 <= x < 30.0 and 40 < y <= 100:
        return '25-29'
    elif 30.0 <= x < 35.0 and 40 < y <= 100:
        return '30-34'
    elif 35.0 <= x < 40.0 and 40 < y <= 100:
        return '35-39'
    elif 40.0 <= x < 45.0 and 40 < y <= 100:
        return '40-44'
    elif 45.0 <= x < 50.0 and 40 < y <= 100:
        return '45-49'
    elif 50.0 <= x < 55.0 and 40 < y <= 100:
        return '50-54'
    elif 55.0 <= x < 60.0 and 40 < y <= 100:
        return '55-59'
    elif 60.0 <= x < 65.0 and 40 < y <= 100:
        return '60-64'
    elif 65.0 <= x < 70.0 and 40 < y <= 100:
        return '65-69'
    elif 70.0 <= x < 75.0 and 40 < y <=100:
        return '70-74'
    elif 75.0 <= x < 80.0 and 40 < y <=100:
        return '75-79'
    elif 80.0 <= x < 85.0 and 40 < y <= 100:
        return '80-84'
    elif 85.0 <= x < 90.0 and 40 < y <= 100:
        return '85-90'
    elif 90.0 <= x < 95.0 and 40 < y <= 100:
        return '90-94'
    elif 95.0 <= x and 40 < y <= 100:
        return '≥95'

    # N = 21-40
    elif 11.0 > x and 20 < y <= 40:
        return '≤ 10'
    elif 11.0 <= x < 20.0 and 20 < y <= 40:
        return '11-19'
    elif 20.0 <= x < 30.0 and 20 < y <= 40:
        return '20-29'
    elif 30.0 <= x < 40.0 and 20 < y <= 40:
        return '30-39'
    elif 40.0 <= x < 50.0 and 20 < y <= 40:
        return '40-49'
    elif 50.0 <= x < 60.0 and 20 < y <= 40:
        return '50-59'
    elif 60.0 <= x < 70.0 and 20 < y <= 40:
        return '60-69'
    elif 70.0 <= x < 80.0 and 20 < y <=40:
        return '70-79'
    elif 80.0 <= x < 90.0 and 20 < y <=40:
        return '80-89'
    elif 90.0 <= x and 20  < y <= 40:
        return '≥90'


# apply masking rule
WEB_SBA_SPAN['check'] = WEB_SBA_SPAN.apply(lambda x: masking_rule(x['Level 1'], x['N']), axis = 1)
WEB_SBA_SPAN['check2'] = WEB_SBA_SPAN.apply(lambda x: masking_rule(x['Level 2'], x['N']), axis = 1)
WEB_SBA_SPAN['check3'] = WEB_SBA_SPAN.apply(lambda x: masking_rule(x['Level 3'], x['N']), axis = 1)
WEB_SBA_SPAN['check4'] = WEB_SBA_SPAN.apply(lambda x: masking_rule(x['Level 4'], x['N']), axis = 1)

# N - 10 - 20
def masking_rule2(x, y, z):
    if z < 21:
        return str(x + y)

# apply masking rule 2
WEB_SBA_SPAN['check5'] = WEB_SBA_SPAN.apply(lambda x: masking_rule2(x['Level 1'], x['Level 2'], x['N']), axis=1)
WEB_SBA_SPAN['check6'] = WEB_SBA_SPAN.apply(lambda x: masking_rule2(x['Level 3'], x['Level 4'], x['N']), axis=1)

# formatting and removing columns
WEB_SBA_SPAN.loc[(WEB_SBA_SPAN['check'].isnull()) & (WEB_SBA_SPAN['N'] < 21), 'check'] = '^'
WEB_SBA_SPAN.loc[(WEB_SBA_SPAN['check4'].isnull()) & (WEB_SBA_SPAN['N'] < 21), 'check4'] = '^'

WEB_SBA_SPAN = WEB_SBA_SPAN.astype({'check5':float})
WEB_SBA_SPAN = WEB_SBA_SPAN.astype({'check6':float})

WEB_SBA_SPAN.loc[WEB_SBA_SPAN['check5'] >= 80.0, 'check2'] = '≥ 80'
WEB_SBA_SPAN.loc[WEB_SBA_SPAN['check6'] <= 20.0, 'check3'] = '≤ 20'

# Duplicated indexes.. reset index
#print(WEB_SBA_SPAN[WEB_SBA_SPAN.index.duplicated()])
WEB_SBA_SPAN_FINAL = WEB_SBA_SPAN.reset_index()

WEB_SBA_SPAN_FINAL.loc[(WEB_SBA_SPAN_FINAL['check2'].isnull()) & (WEB_SBA_SPAN_FINAL['N'] < 21), 'check2'] = WEB_SBA_SPAN_FINAL['check5']
WEB_SBA_SPAN_FINAL.loc[(WEB_SBA_SPAN_FINAL['check3'].isnull()) & (WEB_SBA_SPAN_FINAL['N'] < 21), 'check3'] = WEB_SBA_SPAN_FINAL['check6']

# remove .0
WEB_SBA_SPAN_FINAL['check'] = WEB_SBA_SPAN_FINAL['check'].astype(str).replace('\.0', '', regex=True)
WEB_SBA_SPAN_FINAL['check2'] = WEB_SBA_SPAN_FINAL['check2'].astype(str).replace('\.0', '', regex=True)
WEB_SBA_SPAN_FINAL['check3'] = WEB_SBA_SPAN_FINAL['check3'].astype(str).replace('\.0', '', regex=True)
WEB_SBA_SPAN_FINAL['check4'] = WEB_SBA_SPAN_FINAL['check4'].astype(str).replace('\.0', '', regex=True)

# rename columns
WEB_SBA_SPAN_FINAL.rename(columns={'check':'Level 1 %'}, inplace=True)
WEB_SBA_SPAN_FINAL.rename(columns={'check2':'Level 2 %'}, inplace=True)
WEB_SBA_SPAN_FINAL.rename(columns={'check3':'Level 3 %'}, inplace=True)
WEB_SBA_SPAN_FINAL.rename(columns={'check4':'Level 4 %'}, inplace=True)


# Order and Select Columns by index
WEB_SBA_SPAN_FINAL = WEB_SBA_SPAN_FINAL.iloc[:, np.r_[1,2,3,9,10,11,12]]

WEB_SBA_SPAN_FINAL.drop(WEB_SBA_SPAN_FINAL[WEB_SBA_SPAN_FINAL.District== 'State Charter'].index, inplace=True)

# encoding utf-8 producing undesirable characters.. use utf-8-sig or utf-16
# excel converting strings to dates, save file as txt as workaround
WEB_SBA_SPAN_FINAL.to_csv("WEB_SBA_SPAN_FINAL.txt", sep=',', encoding='utf-8-sig', index = False)


####### SOAP FILES #####################################################################################################

# School Level
df_soap = merged_schools.copy()
df_soap['All_Stu'] = 1.0
df_soap['Sort_Code'] = str()


# Sort_Code_Mappings
Allstu_map = {1.0:'1'}
Female_map = {'Female':'2'}
Male_map = {'Male':'3'}
White_map = {'Caucasian':'4'}
Black_map = {'Black or African American':'5'}
Asian_map = {'Asian':'7'}
Native_map = {'American Indian/Alaskan Native':'8'}
Hawaiian_map = {'Native Hawaiian or Other Pacific Islander':'7'}
FRL_map = {'R':'9'}
FRL_mapII = {'F':'9'}
SWD_map = {'Y':'10'}
ELL_map = {'Y':'11'}
Homeless_map = {'Homeless':'12'}
Military_map = {'Active':'13'}
Military_mapII = {'National Guard':'13'}
Foster_map = {'Y':'14'}
Migrant_map = {'Y':'15'}

# apply maps
df_soap['Sort_Code'] = df_soap['All_Stu'].map(Allstu_map) #map function applied with map rules
df_soap1 = df_soap.copy()
df_soap1['Sort_Code'] = df_soap['S_GENDER'].map(Female_map).copy() #map function applied with map rules
df_soap2 = df_soap.copy()
df_soap2['Sort_Code'] = df_soap['S_GENDER'].map(Male_map) #map function applied with map rules
df_soap3 = df_soap.copy()
df_soap3['Sort_Code'] = df_soap['S_ETNICITY'].map(White_map) #map function applied with map rules
df_soap4 = df_soap.copy()
df_soap4['Sort_Code'] = df_soap['S_ETNICITY'].map(Black_map) #map function applied with map rules
df_soap6 = df_soap.copy()
df_soap6['Sort_Code'] = df_soap['S_ETNICITY'].map(Asian_map) #map function applied with map rules
df_soap7 = df_soap.copy()
df_soap7['Sort_Code'] = df_soap['S_ETNICITY'].map(Native_map) #map function applied with map rules
df_soap17 = df_soap.copy()
df_soap17['Sort_Code'] = df_soap['S_ETNICITY'].map(Hawaiian_map) #map function applied with map rules
df_soap8 = df_soap.copy()
df_soap8['Sort_Code'] = df_soap['S_FRLP'].map(FRL_map) #map function applied with map rules
df_soap9 = df_soap.copy()
df_soap9['Sort_Code'] = df_soap['S_FRLP'].map(FRL_mapII) #map function applied with map rules
df_soap10 = df_soap.copy()
df_soap10['Sort_Code'] = df_soap['S_SPECIAL_ED'].map(SWD_map) #map function applied with map rules
df_soap11 = df_soap.copy()
df_soap11['Sort_Code'] = df_soap['S_ELL_STATUS'].map(ELL_map) #map function applied with map rules
df_soap12 = df_soap.copy()
df_soap12['Sort_Code'] = df_soap['S_HOMELESS'].map(Homeless_map) #map function applied with map rules
df_soap13 = df_soap.copy()
df_soap13['Sort_Code'] = df_soap['S_MILITARY'].map(Military_map) #map function applied with map rules
df_soap14 = df_soap.copy()
df_soap14['Sort_Code'] = df_soap['S_MILITARY'].map(Military_mapII) #map function applied with map rules
df_soap15 = df_soap.copy()
df_soap15['Sort_Code'] = df_soap['S_FOSTER'].map(Foster_map) #map function applied with map rules
df_soap16 = df_soap.copy()
df_soap16['Sort_Code'] = df_soap['S_MIGRANT'].map(Migrant_map) #map function applied with map rule

# merge ethnic codes
df_soap_merge_eth = pd.concat([df_soap3,df_soap4,df_soap6,df_soap7], axis =0)

# Drop NaNs from ethnic groups
df_soap_merge_eth = df_soap_merge_eth.dropna(subset = ['Sort_Code'])

# override with the Hispanic Indicator
df_soap_merge_eth.loc[df_soap_merge_eth['S_HISPANIC_INDICATOR'] == 'Yes', 'Sort_Code'] = '6'

#stack data frames to merge all sort codes
df_soap_merge = pd.concat([df_soap,df_soap1,df_soap2,df_soap_merge_eth,df_soap8,df_soap9,df_soap10,df_soap11,df_soap12,df_soap13,df_soap14,df_soap15,df_soap16], axis =0)

# Drop NaNs from soap_merge
df_soap_merge = df_soap_merge.dropna(subset = ['Sort_Code'])

# School level
df_soap_school = df_soap_merge[['Code','District','School','Sort_Code','PL']].copy()
df_soap_school = (df_soap_school.groupby(['Code','District','School','Sort_Code','PL'])['PL'].count().reset_index(name='N_PL')) #reset index moves index to columns
df_soap_school= df_soap_school.pivot_table(index=['Code','District','School','Sort_Code'] , columns='PL', values='N_PL',
                    aggfunc ='sum', margins=True, dropna=True, fill_value=0)
df_soap_school_index = df_soap_school.reset_index()

# District Level
df_soap_district = df_soap_merge[['DisCode','District','Sort_Code','PL']].copy()
df_soap_district['School'] = 'Districtwide'
df_soap_district['Code'] = df_soap_district['DisCode'] * 1000
df_soap_district = (df_soap_district.groupby(['Code','District','School','Sort_Code','PL'])['PL'].count().reset_index(name='N_PL')) #reset index moves index to columns
df_soap_district= df_soap_district.pivot_table(index=['Code','District','School','Sort_Code'] , columns='PL', values='N_PL',
                    aggfunc ='sum', margins=True, dropna=True, fill_value=0)
df_soap_district_index = df_soap_district.reset_index()

# State Level
df_soap_state = df_soap_merge[['S_DISTRICT_CODE','District','Sort_Code','PL']].copy()
df_soap_state['School'] = 'All Students'
df_soap_state['Code'] = 999999
df_soap_state['District'] = 'Statewide'
df_soap_state = (df_soap_state.groupby(['Code','District','School','Sort_Code','PL'])['PL'].count().reset_index(name='N_PL')) #reset index moves index to columns
df_soap_state= df_soap_state.pivot_table(index=['Code','District','School','Sort_Code'] , columns='PL', values='N_PL',
                    aggfunc ='sum', margins=True, dropna=True, fill_value=0)
df_soap_state_index = df_soap_state.reset_index()


# merge school, district, and state levels
df_soap_merge_final = pd.concat([df_soap_school_index,df_soap_district_index,df_soap_state_index], axis =0)

#check columns names
df_soap_merge_final.rename(columns={1.0:'Level 1'}, inplace=True)
df_soap_merge_final.rename(columns={2.0:'Level 2'}, inplace=True)
df_soap_merge_final.rename(columns={3.0:'Level 3'}, inplace=True)
df_soap_merge_final.rename(columns={4.0:'Level 4'}, inplace=True)

# function for calculating percentage
def division_rule(x, y):
    return (x / y) * 100

df_soap_merge_final['check'] = df_soap_merge_final.apply(lambda x: division_rule(x['Level 1'], x['All']), axis=1)
df_soap_merge_final['check1'] = df_soap_merge_final.apply(lambda x: division_rule(x['Level 2'], x['All']), axis=1)
df_soap_merge_final['check2'] = df_soap_merge_final.apply(lambda x: division_rule(x['Level 3'], x['All']), axis=1)
df_soap_merge_final['check3'] = df_soap_merge_final.apply(lambda x: division_rule(x['Level 4'], x['All']), axis=1)

# calculate proficiency
df_soap_merge_final['Proficient'] = df_soap_merge_final['check2'] + df_soap_merge_final['check3']

# Round to nearest tenth
df_soap_merge_final['check'] = df_soap_merge_final['check'].round(1)
df_soap_merge_final['check1'] = df_soap_merge_final['check1'].round(1)
df_soap_merge_final['check2'] = df_soap_merge_final['check2'].round(1)
df_soap_merge_final['check3'] = df_soap_merge_final['check3'].round(1)
df_soap_merge_final['Proficient'] = df_soap_merge_final['Proficient'].round(1)

# Order and Select Columns by index
df_soap_merge_final = df_soap_merge_final.iloc[:, np.r_[0,1,2,3,8,9,10,11,12,13]]

# Remove 'All' Rows & State Charters if 1 school per district
df_soap_merge_final.drop(df_soap_merge_final[df_soap_merge_final.Code == 'All'].index, inplace=True)
df_soap_merge_final.drop(df_soap_merge_final[df_soap_merge_final.District== 'All'].index, inplace=True)
df_soap_merge_final.drop(df_soap_merge_final[df_soap_merge_final.District== 'State Charter'].index, inplace=True)

#check columns names
df_soap_merge_final.rename(columns={'check':'Level 1'}, inplace=True)
df_soap_merge_final.rename(columns={'check1':'Level 2'}, inplace=True)
df_soap_merge_final.rename(columns={'check2':'Level 3'}, inplace=True)
df_soap_merge_final.rename(columns={'check3':'Level 4'}, inplace=True)
df_soap_merge_final.rename(columns={'All':'N'}, inplace=True)

# Map for sortcodes
sort_code_map = {'1':'ALLSTU','2':'FEMALE','3':'MALE','4':'WHITE','5':'BLACK','6':'HISPANIC','7':'ASIAN','8':'NATIVE',
                 '9':'FRL','10':'SWD','11':'ELL','12':'HOMELESS','13':'MILITARY','14':'FOSTER','15':'MIGRANT'}

df_soap_merge_final['Sort_Code'] = df_soap_merge_final['Sort_Code'].map(sort_code_map) #map function applied with map rule



#df_soap_merge_final.to_csv("DF_SOAP_SBA_SPAN_FINAL.csv", sep=',', encoding='utf-8', index = False)
# Testing VERSION CONTROL!
