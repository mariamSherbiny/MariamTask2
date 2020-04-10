from subprocess import run, PIPE, Popen
from pathlib import Path
import subprocess
import os
from os.path import isfile, join
import argparse
import fnmatch
import json 
import pandas as pd, requests
import numpy as np
from pandas.io.json import json_normalize
import time

start_time = time.time()

checksums = {}
duplicates = []

parser = argparse.ArgumentParser()

parser.add_argument("dir", help = "Directory to list files in")

parser.add_argument("-u", "--unix", action="store_true", dest="unix", default=False, help="To display unix time")

args = parser.parse_args()


scanned_files = os.scandir(args.dir)
for filename in scanned_files:
    if filename.is_file():
        if fnmatch.fnmatch(filename.name, '*.json'):
            with Popen(["md5sum", filename.name], stdout=PIPE) as proc:
                checksum,_ = proc.stdout.read().split()
                if checksum in checksums:
                    duplicates.append(filename.name)
                
                checksums[checksum]= filename.name

                if filename.name in duplicates :
                    print(f"this file: {filename.name} is duplicated")
                else:
                    os.chdir(args.dir)
                    records = [json.loads(line) for line in open(filename.name)]
                    df = json_normalize(records)
                    df = df[['a','tz','r','u','t','hc','cy', 'll']]
                    df.rename(columns = {'a':'Browser','tz':'Time Zone','r':'From_url','u':'To_url','t':'Time_in','hc':'Time_out','cy':'City'},inplace = True)
                    df['OS'] = df['Browser'].str.extract(r"\((.*?)\)", expand=False)
                    df['Browser'] = df['Browser'].str.split('/').str[0]
                    df['OS'] = df['OS'].str.split(';').str[0]
                    df['From_url'] = df['From_url'].str.split('/').str[2]
                    df['To_url'] = df['To_url'].str.split('/').str[2]
                    df.dropna(inplace=True)
                    if not args.unix:
                        creation_timestamp = []
                        for i, row in df.iterrows():
                            stamp = pd.to_datetime(row['Time_in'], unit='s').tz_localize(row['Time Zone']).tz_convert('UTC')
                            creation_timestamp.append(stamp)  
                        df['Time_in'] = creation_timestamp
                    if not args.unix:
                        creation_timestamp_out = []
                        for i, row in df.iterrows():
                            stamp_out = pd.to_datetime(row['Time_out'], unit='s').tz_localize(row['Time Zone']).tz_convert('UTC')
                            creation_timestamp_out.append(stamp_out)  
                        df['Time_out'] = creation_timestamp_out
                    df['longitude']= df['ll'].str[0]
                    df['latitude']= df['ll'].str[1]
                    df.drop('ll', axis=1 ,inplace=True)

                    file_name,file_extension = os.path.splitext(filename.name)
                    file_name= file_name+".csv"
                    df.to_csv(file_name, index = False)
                    print(f"number of rows has bees transformed :  {df['Browser'].count()}")
                    print(f"The file path : {file_name}")
                    print("total time of script execution =  %s seconds " % (time.time() - start_time))
