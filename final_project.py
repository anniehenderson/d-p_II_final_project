import pandas as pd
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
start_file = r'globalterrorismdb_0522dist.xlsx'
start_csv_file = r'START_globalterror.csv'
mj_file = r'Mother Jones - Mass Shootings Database, 1982 - 2022 - Sheet1.csv'
ap_file = r'mass_killing_incidents_public.csv'

# start_xlsx = pd.read_excel(os.path.join(base_path, start_file))
# start_xlsx.to_csv(os.path.join(base_path, start_csv_file), sep='|')
start_csv = pd.read_csv(os.path.join(base_path, start_csv_file),sep='|')

def clean_start(dataframe):
    df = dataframe.iloc[:, np.r_[1:4,6,9,12,19,23,25,26:28,30,35,38,40,42,59,61,63,64:72,81,83,87,97:105,106:108,109:112,119:121,123:125]]
    clean_df = df.loc[(df['country_txt'] == 'United States') & (df['iyear'] > 2010)]
    return clean_df

start = clean_start(start_csv)
    #https://stackoverflow.com/questions/48545076/selecting-multiple-dataframe-columns-by-position-in-pandas

mj_raw = pd.read_csv(os.path.join(base_path, mj_file))
ap_raw = pd.read_csv(os.path.join(base_path, ap_file))

#def make_mass_shooting_df(ap_data,mj_data):
mj = mj_raw[['case', 'location', 'date', 'location.1', 'year']]
mj = mj[mj['year'] > 2010]
mj = mj[mj['year'] < 2021]
mj[['city','state_name']] = mj.location.str.split(', ', expand=True)
mj[['month','day','x_year']] = mj.date.str.split('/', expand=True)
    #https://www.geeksforgeeks.org/split-a-text-column-into-two-columns-in-pandas-dataframe/
mj
###NEED TO TRANSLATE 

ap_raw[['year','month','day']] = ap_raw.date.str.split('-', expand=True)

##NEED TO MATCH MJ TO AP (KEEP ALL AP EVENTS AND ADD DATA TO THEM)


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
    df_columns = ('Location', 'Total Incidents (2020)','2020', '2019', '2018','2017','2016','2015', '2014', '2013', '2012', '2011','x')
    output = pd.DataFrame(for_df, columns = df_columns)
        #https://www.geeksforgeeks.org/creating-pandas-dataframe-using-list-of-lists/
    output = output.drop(columns=['x'])
        #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html
    return output

crime_rate = make_crime_df(wiki_text,states)


