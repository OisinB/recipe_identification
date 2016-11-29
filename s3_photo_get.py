#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 14:45:24 2016

@author: oisin-brogan

Get recipe photos from s3 storge
photo_file should be filepath to a .csv file of image ids from the recipe table
dest_fldr is where you want the photos to end up

Default region is 1 for English
"""

import sys
import boto3
import MySQLdb
from threading import Thread

if len(sys.argv) == 3:
    photo_file, dest_fldr = sys.argv[1:]
    region = 1
else:
    photo_file, dest_fldr, region = sys.argv[1:]

s3 = boto3.resource('s3', region_name='us-east-1',
                   )
tofu = s3.Bucket('tofu.us-east-1')

if photo_file.split('.')[1] == 'csv':    
    with open(photo_file, 'r') as f:
        photo_ls = f.read()
    
    photo_ls = photo_ls.split('\n')[1:] #skip header


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
    photo_ls = [t[0] for t in photo_ls] #convert from tuples
    db.close()
        
else:
    print 'Bad file - please give either .csv list or .sql sql_query'

def get_recipe_photo(key_list, region, dest_folder):
    for key in key_list:
        try:
            tofu.download_file('{0:03d}_recipes/{1}/{1}.jpg'.format(region, key), 
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
    thread = Thread(target=get_recipe_photo, args=(id_range,region,dest_fldr))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join() # wait for completion
