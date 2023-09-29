import numpy as np
import pandas as pd
import matplotlib as plt
import advertools as adv
import ua_parser
from ua_parser import user_agent_parser
import pyarrow
import pyarrow.parquet as pq
from ipywidgets import interact

pd.options.display.max_columns = None



#if the file is not in row and columns
def convert_file(file_path):
    adv.logs_to_df(
        log_file=file_path,
        output_file='output_file.parquet',
        errors_file='errors_file.txt',
        log_format='combined'
    )


#the read the file and convert all variable in proper points
def proper_format(output_file):
    # Read parquet file into dataframe
    logs_df = pd.read_parquet(output_file)

    # Convert datetime column to datetime object
    logs_df['datetime'] = pd.to_datetime(logs_df['datetime'], format='%d/%b/%Y:%H:%M:%S %z', utc=True)

    # Define regex patterns for IP, timestamp, status code, and user agent
    ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
    timestamp_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    status_code_pattern = r'\d{3}'
    user_agent_pattern = r'"User-Agent":"([\w\s\(\)-]+)"'

    # Return dataframe with converted datetime
    return logs_df

#to display first 10 values of data
def data_first_10 (logs_df):
    return logs_df.head(10)

def summary (logs_df):
    return logs_df.describe()

def number_rows_columns(logs_df):
    num_rows, num_columns = logs_df.shape
    return num_rows, num_columns


def unique_values_statuscode(logs_df):
    return logs_df["status_code"].unique()


def top_10_hours (logs_df):
    logs_df['hour'] = logs_df['datetime'].dt.hour
    top_hours = logs_df['hour'].value_counts().head(10)

    plt.bar(top_hours.index, top_hours.values)
    plt.xlabel('Hour')
    plt.ylabel('Number of Hits')
    plt.title('Top 10 Hours for Hits')
    plt.show()
    return top_hours


def http_code_count (logs_df):
    http_code_counts =logs_df['status'].value_counts()
    return http_code_counts

def maximum_hit_url(logs_df):
    url_hit_counts = logs_df['referer'].value_counts()
    max_hit_url = url_hit_counts.idxmax()
    return max_hit_url

def top_hit_platform(logs_df):
    logs_df['platform']= logs_df['user_agent'].str.extract(r'\((.*?)\)')[0].str.split(";").str[0]
    platform_hit_counts = logs_df['platform'].value_counts()
    return platform_hit_counts
