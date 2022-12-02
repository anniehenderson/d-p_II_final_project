import pandas as pd
pd.options.mode.chained_assignment = None
import os
import numpy as np
import requests
from bs4 import BeautifulSoup
import us
import re


#You must use a minimum of three datasets, at least one of which should be retrieved automatically from the web using APIs or web scraping. 
#All processing of the data should be handled by your code, including all merging and reshaping. 
#Any automatic data retrieval must have an option to toggle accessing the web off if the data is already downloaded. 
#This is where you can showcase your abilities practiced in homework 1, and in Data and Programming 1.

base_path = r'/Users/anniehenderson/Desktop/Harris 2022 Q1 Fall/Data & Programming II/Final Project/'
ap_file = r'mass_killing_incidents_public.csv'
start_file = r'globalterrorismdb_0522dist.xlsx'
start_csv_file = r'START_globalterror.csv'

ap_raw = pd.read_csv(os.path.join(base_path, ap_file))

def clean_ap(ap_data):
    ap_data[['year','month','day']] = ap_data.date.str.split('-', expand=True)
        #https://www.geeksforgeeks.org/split-a-text-column-into-two-columns-in-pandas-dataframe/
    ap_data['year'] = ap_data['year'].astype(int)
    ap = ap_data[(ap_data['year'] > 2010) & (ap_data['year'] < 2021)]
    return ap

ap = clean_ap(ap_raw)

# start_xlsx = pd.read_excel(os.path.join(base_path, start_file))
# start_xlsx.to_csv(os.path.join(base_path, start_csv_file), sep='|')
start_csv = pd.read_csv(os.path.join(base_path, start_csv_file),sep='|')

state_names = [state.name for state in us.states.STATES_AND_TERRITORIES]
state_names.extend(['District of Columbia'])
state_abbr = [state.abbr for state in us.states.STATES_AND_TERRITORIES]
state_abbr.extend(['DC'])
state_dic = {state_names[i]: state_abbr[i] for i in range(len(state_names))}
    #https://www.geeksforgeeks.org/python-convert-two-lists-into-a-dictionary/
    
def clean_start(dataframe):
    df = dataframe.iloc[:, np.r_[1:4,6,9,12,19,23,25,26:28,30,35,38,40,42,59,61,63,64:72,81,83,87,97:105,106:108,109:112,119:121,123:125]]
    clean_df = df.loc[(df['country_txt'] == 'United States') & (df['iyear'] > 2010)]
    clean_df['state'] = clean_df['provstate'].replace(state_dic)
    clean_df.rename(columns={'iyear':'year'}, inplace=True)
    return clean_df

start = clean_start(start_csv)
    #https://stackoverflow.com/questions/48545076/selecting-multiple-dataframe-columns-by-position-in-pandas
        
wiki_page = requests.get('https://en.wikipedia.org/wiki/List_of_U.S._states_and_territories_by_violent_crime_rate')
soup = BeautifulSoup(wiki_page.content, 'lxml')
wiki_text = soup.get_text()

states = [state.name for state in us.states.STATES]
    #https://stackoverflow.com/questions/47168881/how-to-get-a-list-of-us-state-names-from-us-1-0-0-package
states.extend(['District of Columbia','Puerto Rico','United States'])

def make_crime_df(text,state_list):
    index_list = []
    for state in state_list:
        if state == 'Virginia' or state == 'United States':
            ind = [m.start() for m in re.finditer(state, text)]
                #https://www.tutorialspoint.com/How-do-we-use-re-finditer-method-in-Python-regular-expression
            index_list.append(ind[1])
        else:
            ind = text.find(state)
            index_list.append(ind)
    index_list.sort()
        #https://www.geeksforgeeks.org/python-list-sort-method/ 
    end = text.find('See also')
    index_list.append(end)
    data_clean = []
    for item in index_list:
        if index_list.index(item) < 53:
            end_ind = int(index_list.index(item)+1)
            data = text[int(item):int(index_list[end_ind])]
            data = data.replace('\n', '|').replace('|||', '|').replace('||', '|')
                #https://stackoverflow.com/questions/16566268/remove-all-line-breaks-from-a-long-string-of-text
            data_clean.append(data)
        else:
            continue
    for_df = []
    for string in data_clean:
        data_list = list(string.split('|'))
        for_df.append(data_list)
    df_columns = ('location', 'total_incidents_2020','2020', '2019', '2018','2017','2016','2015', '2014', '2013', '2012', '2011','x')
    output = pd.DataFrame(for_df, columns = df_columns)
        #https://www.geeksforgeeks.org/creating-pandas-dataframe-using-list-of-lists/
    output = output.drop(columns=['x'])
        #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html
    return output

crime_rate = make_crime_df(wiki_text,states)
crime_rate['state'] = crime_rate['location'].replace(state_dic)

def make_model_data(start_data, crime_data):
    start_long = start_data.groupby(['year','state']).size()
    start_long = start_long.to_frame(name='dom_ter_incidents')
    start_long = start_long.reset_index()
    start_long = start_long.astype({'year':'int','state':'str','dom_ter_incidents':'float'})
    
    crime_clean = crime_data.drop(columns=['total_incidents_2020'])
    year_list = ['2011','2012','2013','2014','2015','2016','2017','2018','2019','2020']
    crime_long = pd.melt(crime_clean, id_vars='state', value_vars=year_list)
    crime_long.columns = ('state','year','crime_rate')
    crime_long = crime_long.astype({'state':'str','year':'int','crime_rate':'float'})
    
    df = pd.merge(crime_long, start_long, on=['year','state'])
    return df

model_df = make_model_data(start, crime_rate)
   
model_df_path = r'model_df.csv'
model_df.to_csv(os.path.join(base_path, model_df_path)) 

def make_annual_df(start_data, ap_data, crime_data):    
    start_agg = start.groupby('year')['state'].count()
    start_agg = start_agg.to_frame(name=None)
    start_agg = start_agg.reset_index()
    start_agg.columns = ('year','dom_ter_incidents')
    start_agg = start_agg.astype({'year':'int','dom_ter_incidents':'float'})
    
    ap_agg = ap.groupby('year')['state'].count()
    ap_agg = ap_agg.to_frame(name=None)
    ap_agg = ap_agg.reset_index()
    ap_agg.columns = ('year','mass_shootings')
    ap_agg = ap_agg.astype({'year':'int','mass_shootings':'float'})
    
    crime = crime_data.drop(columns=['total_incidents_2020'])
    crime.set_index('location',inplace=True)
    crime_tran = crime.transpose()
        #https://www.w3resource.com/pandas/dataframe/dataframe-transpose.php#:~:text=The%20transpose()%20function%20is,as%20columns%20and%20vice%2Dversa.&text=If%20True%2C%20the%20underlying%20data,copy%20is%20made%20if%20possible.
    crime_tran = crime_tran.reset_index()
    crime_agg = crime_tran[['index','United States']]
    crime_agg.columns = ('year','crime_rate')
    crime_agg = crime_agg.astype({'year':'int','crime_rate':'float'})
    
    df = pd.merge(crime_agg, start_agg, on='year')
    final = df.merge(ap_agg, on=['year'])
    return final

annual_national = make_annual_df(start, ap, crime_rate)

annual_national_path = r'annual_df.csv'
annual_national.to_csv(os.path.join(base_path, annual_national_path))
