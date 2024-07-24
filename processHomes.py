import os
import pandas as pd
import dask.dataframe as dd
import glob
from sys import argv

pd.options.mode.chained_assignment = None

file_name = argv[1].split('.')[0]
ext = ".tsv"
file_path = file_name+ext

# Define the start and end hours for filtering
start_hour = 1
end_hour = 4

threshold=15

# Function to convert Unix timestamps to seconds (if not already in seconds)
def convert_timestamp_to_dt(ddf, tz='US/Central'):        
    ddf['DateTime'] = dd.to_datetime(ddf['Unix Timestamp'],unit='s')    
    ddf.DateTime = ddf.DateTime.dt.tz_localize('UTC').dt.tz_convert(tz)  
    ddf['Date'] = ddf.DateTime.dt.date
    return ddf

def keep_rows_between_hours(df, start_hour, end_hour):
    df = df[(df['DateTime'].dt.hour >= start_hour) & (df['DateTime'].dt.hour <= end_hour)]
    return df

def chunk(s):
    # for the comments, assume only a single grouping column, the 
    # implementation can handle multiple group columns.
    #
    # s is a grouped series. value_counts creates a multi-series like 
    # (group, value): count
    return s.value_counts()


def agg(s):
    # s is a grouped multi-index series. In .apply the full sub-df will passed
    # multi-index and all. Group on the value level and sum the counts. The
    # result of the lambda function is a series. Therefore, the result of the 
    # apply is a multi-index series like (group, value): count

    s = s._selected_obj
    return s.groupby(level=list(range(s.index.nlevels))).sum()


def finalize(s):
    # s is a multi-index series of the form (group, value): count. First
    # manually group on the group part of the index. The lambda will receive a
    # sub-series with multi index. Next, drop the group part from the index.
    # Finally, determine the index with the maximum value, i.e., the mode.
    level = list(range(s.index.nlevels - 1))
    return (
        s.groupby(level=level)
        .apply(lambda s: s.reset_index(level=level, drop=True).idxmax())
    )

# Read the large TSV file in chunks using Dask
columns=["Polygon_ID","Device_ID","Lat","Lon","Unix Timestamp"]
df = dd.read_csv(file_path, sep='\t', names=columns, header=None,
                 dtype={"Polygon_ID":'object',"Device_ID":'object',"Lat":'float64',"Lon":'float64',"Unix Timestamp":'object'}).compute()

ddf = dd.from_pandas(df, npartitions=256)

meta_dict = {"Polygon_ID":'object',"Device_ID":'object',"Lat":'float64',"Lon":'float64',"Unix Timestamp":'object', 'DateTime':'object','Date':'object'}

# Convert Unix timestamps to seconds
ddf = ddf.map_partitions(convert_timestamp_to_dt, meta=meta_dict)

# Drop rows between specific hours
ddf = ddf.map_partitions(keep_rows_between_hours, start_hour=start_hour, end_hour=end_hour,
                        meta=meta_dict)

max_occurence = dd.Aggregation('mode', chunk, agg, finalize)

# Group by timestamp and ID, then aggregate
grouped_df = ddf.groupby(['Device_ID','Date']).agg({
    'Lat': max_occurence,
    'Lon': max_occurence
}).reset_index()

v = grouped_df['Device_ID'].value_counts().compute()
grouped_df = grouped_df[grouped_df['Device_ID'].isin(v.index[v.gt(threshold)])]

# Group by timestamp and ID, then aggregate as needed
grouped_df = grouped_df.groupby(['Device_ID']).agg({
    'Lat': max_occurence,
    'Lon': max_occurence
}).reset_index()

grouped_df.compute().to_csv(file_name+'_output.tsv', sep='\t', index=False)#, single_file=True)
