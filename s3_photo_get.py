#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 14:45:24 2016

@author: oisin-brogan

Get recipe photos from s3 storge
photo_file should be filepath to a .csv file of image ids and provider ids,
with the columns in that order
    OR it can be an sql queru file, assmuing you have a local copy of the db
dest_fldr is where you want the photos to end up
Default s3_folder is recipes. Other values might be photo_reports, steps etc.

NB for sql: the sql query assumes you're using the global_development db.
If you need to use tables in other dbs (e.g. global_stats_devleopment for 
keywords from search), make sure to speficiy such in your query
"""

import sys
import boto3
import MySQLdb
from threading import Thread

if len(sys.argv) == 2:
    photo_file, dest_fldr = sys.argv[1:]
    s3_folder = 'recipes'
else:
    photo_file, dest_fldr, s3_folder = sys.argv[1:]

s3 = boto3.resource('s3', region_name='us-east-1',
                    
                    )
tofu = s3.Bucket('tofu.us-east-1')

if photo_file.split('.')[1] == 'csv': 
    with open(photo_file, 'r') as f:
        photo_ls = f.read()
    
    photo_ls = photo_ls.split('\n')[1:] #skip header
    photo_ls = [s.strip().split(',') for s in photo_ls]
    

elif photo_file.split('.')[1] == 'sql':
    db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     db="global_development") # name of the data base

    cur = db.cursor()
    
    with open(photo_file, 'r') as f:
        query = f.read()
    
    # Use all the SQL you like
    cur.execute(query)
    photo_ls = cur.fetchall()
    #photo_ls = [t[0] for t in photo_ls] #convert from tuples
    photo_ls = [p for p in photo_ls if p] #drop NULLs
    db.close()
        
else:
    print 'Bad file - please give either .csv list or .sql sql_query'

def get_photo(tup_list, dest_folder, s3_folder):
    for tup in tup_list:
        region, key = tup
        region = int(region)
        try:
            tofu.download_file('{0:03d}_{2}/{1}/{1}.jpg'.format(region, key, s3_folder), 
                           '{0}/{1}.jpg'.format(dest_folder, key))
        except:
            import traceback
            print "{} failed to download".format(key)
            traceback.print_exc()
            
threads = []
nb_threads = 8
num_images = len(photo_ls)
    
for i in range(nb_threads):
    id_range = photo_ls[i*num_images//nb_threads : (i+1)*num_images//nb_threads + 1]
    thread = Thread(target=get_photo, args=(id_range,dest_fldr, s3_folder))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join() # wait for completion
