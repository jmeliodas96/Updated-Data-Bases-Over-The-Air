# Script for get information from www.mcc-mnc.com
# This Script compare two list, the first list for store the external content and second list store the local content from database(table lista)
# After this is load in DataFrames for process and compare every row.
#By Jimmy Mena

# dependences to install

import re
import urllib2
import json
import pandas as pd
import sys
import os
import subprocess
import sched
import time
import datetime
import numpy as np
import psycopg2
from pprint import pprint
import csv

# scheduler show the date when begin the script
scheduler = sched.scheduler(time.time, time.sleep)

# postgresql credentials
DBNAME = "deviceinfo"
USER = "Jimmy"
HOST = "localhost"
PASSWORD = "goku"
FILE = "data.csv"


# Web Scraping for get information
def getCarrierInfo():
    # expression regular for find content between <td>' '</td>
    td_re = re.compile('<td>([^<]*)</td>'*6)
    html = urllib2.urlopen('http://mcc-mnc.com/').read()
    # tbody_start begin the ran on <tbody></tbody>
    tbody_start = False
    # dictionary
    mcc_mnc_list = []
    # account for generate id
    i = 0

    # for every line in the document html, make a split('\n')
    for line in html.split('\n'):
        if '<tbody>' in line:
            tbody_start = True
        elif '</tbody>' in line:
            break
        # elif tbody_start is true
        elif tbody_start:
            i = i + 1
            # find into the line in td
            td_search = td_re.search(line)
            # list temp
            current_item = {}
            # for ran the line and find the mnc,mcc,iso[1:2:3]
            td_search = td_re.split(line)

            # generate key_carrier(mcc,mnc)
            one = current_item['mcc'] = td_search[1]
            two = current_item['mnc'] = td_search[2]
            current_item['iso'] = td_search[3]
            current_item['country'] = td_search[4]
            current_item['countryc'] = td_search[5]
            current_item['network'] = td_search[6][0:-1]
            current_item['key_carrier'] = one + two
            current_item['id'] = i

            # insert
            mcc_mnc_list.append(current_item)

	# generate out ext.json
    with open('ext.json', 'w') as file:
        json.dump(mcc_mnc_list, file, indent=2)


# get carrier_list from table lista into database deviceinfo
def getCarrierList():
    os.putenv('PGPASSWORD', '{0}'.format(PASSWORD))
    command = 'psql -U {2} -d {1} -h {0} -p 5432 -t -A -c "COPY (select * from lista) TO STDOUT CSV HEADER" > lista.csv'.format(HOST,DBNAME,USER)
    subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.readlines()
    carrier_list = pd.read_csv('lista.csv')
    unless = ['id', 'mccint', 'mncint']
    for item in unless:
    	del(carrier_list[item])
    return carrier_list

# get external list from local directory, file external.json
def getExtList():
    ext_list = pd.read_json('ext.json',orient='records')
    unless = ['id','key_carrier']
    for item in unless:
        del(ext_list[item])
    return ext_list

# get external list from local directory, file external.json
def getNewList():
    new_list = pd.read_json('new_data_update.json',orient='records')
    return new_list

def parser_data():

    # start script date
    print('Updating data start :  ', str(datetime.datetime.now()))


    # calling functions for get data
    getCarrierInfo()
    external_list = getExtList()
    local_list = getCarrierList()

    #sorting list before load in dataframe
    external_list = external_list[['mcc','mnc','iso','country','countryc','network']]
    local_list = local_list[['mcc','mnc','iso','country','countryc','network']]


    # choose the column into dataframes
    df1 = pd.DataFrame(external_list,columns=['mcc','mnc','iso','country','countryc','network'])
    df2 = pd.DataFrame(local_list,columns=['mcc','mnc','iso','country','countryc','network'])

    # replace elements for values 0 into holl empty
    df1 = df1.fillna(0)
    df2 = df2.fillna(0)


    # columns to convert at integer
    colsdf1 = ['mcc','mnc']
    colsdf2 = ['mcc','mnc']


    # convert column mcc and mnc to base int64
    df2[colsdf2] = df2[colsdf2].dropna().apply(np.int64)
    df1[colsdf1] = df1[colsdf1].dropna().apply(np.int64)


    # this is the total number of index in the table lista from database
    total_index_df1 = (len(df1))
    total_index_df2 = (len(df2))


    # printing two dataframes
    print df1
    print '-----------------------------------------------------------'
    print df2
    print '-----------------------------------------------------------'

	# dictionarys for store the new data updated
    data_update = []

    # accounter for get result
    rigth = 0
    wrong = 0
    empty_count = 0
    i = 0

    #over iterate on data frame 1
    for index_a, row_a in df1.iterrows():

        current_not_exist = {}
        print '--------------'
        print 'DataFrame One : ',index_a, row_a['mcc']
        print '--------------'
        #over iterate on data frame 2
        for index_b, row_b in df2.iterrows():

            print '--------------'
            print 'DataFrame Two : ', index_b, row_b['mcc']
            print (row_a['mcc'],row_a['mnc'])
            print '--------------'
            print (row_b['mcc'],row_b['mnc'])

            # if key_carrier(mcc,mnc) in df1 exist into df2, so increment the account rigth
            if(df1['mcc'].values[index_a] == df2['mcc'].values[index_b]) and (df1['mnc'].values[index_a] == df2['mnc'].values[index_b]):
                # counter for get the rigth conditions is true
            	print 'ok, this exist '
                rigth = rigth + 1
            #elif key_carrier(mcc,mnc) in df2 is empty, so increment the account empty_count
            elif((df2['mcc'].values[index_b] == {} and df2['mnc'].values[index_b] == {}).any()):
            	print 'empty'
            	empty_count = empty_count + 1
            # else, the key_carrier(mcc,mnc) in df1 doesn't exist in df2, get the values of index and insert the new values in df
            elif(index_b >= total_index_df2):
            	print 'this index in df2 does not exist'
            	break
            else:
            	print 'no ok'
            	wrong = wrong + 1



    # end of function parser_data()
    #-----------------------------------------------------------------------------------------


    print '::::::::::::::::::::::::::::::::::::::::::::'
    print '------ Total amount extracted from mcc-mnc.com ---------> : ',   total_index_df1
    print '::::::::::::::::::::::::::::::::::::::::::::'

    print '::::::::::::::::::::::::::::::::::::::::::::'
    print '------ Total amount extracted from Data Base ---------> : ',     total_index_df2
    print '::::::::::::::::::::::::::::::::::::::::::::'

    if(rigth == total_index_df1):

        print 'All logs are rigth, its dont need update, total rigth : ',   rigth
    # if the amount found is < that total logs, so print the amount
    elif(rigth < total_index_df1):
        print 'values rigth : ', rigth
        total	=	total_index_df1 - rigth
        print 'The total logs that needs insert in table lista are : ',             total
        for index_a,row_a in df1.iterrows():
            current_item = {}
            # if rigth is menor to index_a, rigth content the total rigth, index_a start from index_a = 0, here begin get values and insert the new data
            if(rigth < index_a) :
                # get values
                mcc		 = df1['mcc'].values[index_a]
                mnc 	 = df1['mnc'].values[index_a]
                network  = df1['network'].values[index_a]
                iso 	 = df1['iso'].values[index_a]
                country	 = df1['country'].values[index_a]
                countryc = df1['countryc'].values[index_a]

                # insert
                current_item['mcc'] 	 = mcc
                current_item['mnc'] 	 = mnc
                current_item['network']  = network
                current_item['iso'] 	 = iso
                current_item['country']  = country
                current_item['countryc'] = countryc

                data_update.append(current_item)
            else:
                i = i + 1

    else:
    	print '-------l--------'


    # generate a new_data_update.json with content of data_update[]
    with open('new_data_update.json', 'w') as file:
    	json.dump(data_update, file, indent=2)

    # begin the Conecction at database
    status_connection = True
    try:
        conn = psycopg2.connect("dbname='"+  DBNAME +"' user='"+ USER +"' host='"+ HOST +"' password='"+ PASSWORD +"'")
    except psycopg2.DataError as e:
        status_connection = False
        logger.error("Check the parameter to connect to postgresql!! -> %s",e.pgerror)

    if status_connection :
        print 'Conecction successful'

        cursor = conn.cursor()

        # getting new values for table
        new_list = getNewList()

        new_list = new_list[['mcc','mnc','iso','country','countryc','network']]
        df3 = pd.DataFrame(new_list, columns=['mcc', 'mnc', 'iso', 'country',  'countryc', 'network'])

        # generate a csv file
        df3.to_csv('data.csv', header=True, encoding='utf-8', index=False)

        # inserting
        os.putenv('PGPASSWORD', '{0}'.format(PASSWORD))
        command0 = 'psql -U {0} -d {1} -h {2} -p 5432 -t -A -c "\COPY lista(mcc,mnc,iso,country,countryc,network) FROM \'/home/kousei/Task/Last_update/other/{3}\' DELIMITER \',\' CSV HEADER"'.format(USER,DBNAME,HOST,FILE)
        subprocess.Popen(command0, shell=True, stdout=subprocess.PIPE).stdout.readlines()

        print 'The update is finish!!! Good day :)!'
    else:
        print('No connect')



	print '::::::::::::::::::::::::::::::::::::::::::::'
	print 'The total amount key_carrier founds : ', rigth, ' in DF1 --->> wrong amount :: ', wrong
	print '::::::::::::::::::::::::::::::::::::::::::::'
	print 'The total key_carriers ran in DF2 are :: ', total_index_df2, ' row , of which -> ', ((wrong) / (total_index_df1)),' were incorrect in DF1(external content) and ', rigth ,' were correct in DF1(external content)--> of a total of -->> ', total_index_df1, ' row in DF1'

	# in the case that : field mcc and mnc is empty, count show a how many are empty
	print 'The total key_carries empty are : ', empty_count
	print ' : ', data_update



if(__name__ == '__main__'):
    scheduler.enter(0, 1, parser_data, ())
    scheduler.run()
