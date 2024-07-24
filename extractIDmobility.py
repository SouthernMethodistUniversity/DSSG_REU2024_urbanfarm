import os
import pandas as pd
import dask.dataframe as dd
import glob
import numpy as np
from sys import argv

pd.options.mode.chained_assignment = None  # default='warn'

file_name = argv[1].split('.')[0]
ext = ".tsv"
file_path = file_name+ext

# Function to convert Unix timestamps to seconds (if not already in seconds)
def convert_timestamp_to_dt(ddf, tz='US/Central'):        
    ddf['DateTime'] = dd.to_datetime(ddf['Unix Timestamp'],unit='s')    
    #ddf['DateTime'] = ddf["DateTime"].round("H")
    ddf.DateTime = ddf.DateTime.dt.tz_localize('UTC').dt.tz_convert(tz)  
    #ddf['DateTime']=ddf['DateTime'].astype(str)
    #ddf.DateTime = ddf.DateTime.str.slice(stop=-6)
    ddf['Date'] = ddf.DateTime.dt.date
    return ddf

# Read the large TSV file in chunks using Dask
columns=["Polygon_ID","Device_ID","Lat","Lon","Unix Timestamp"]
df = dd.read_csv(file_path, sep='\t', names=columns, header=None,
                 dtype={"Polygon_ID":'object',"Device_ID":'object',"Lat":'float64',"Lon":'float64',"Unix Timestamp":'object'}).compute()

ddf = dd.from_pandas(df, npartitions=256)

meta_dict = {"Polygon_ID":'object',"Device_ID":'object',"Lat":'float64',"Lon":'float64',"Unix Timestamp":'object', 'DateTime':'object','Date':'object'}

# Convert Unix timestamps to seconds
ddf = ddf.map_partitions(convert_timestamp_to_dt, meta=meta_dict)

ids = pd.read_csv("../finalListOfDeviceIDsAtThreeMonthThreshold.txt")

mob = ddf[ddf.Device_ID.isin(ids.Device_ID)].compute()

mob.to_csv(file_name+'_mobility_output.tsv', sep='\t', index=False)