import boto3
import pandas as pd
from sqlalchemy import create_engine

#Create Session
session = boto3.Session(
    aws_access_key_id="AKIAZZ33YB65GZIN656A",
    aws_secret_access_key="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"

)

s3 = session.resource('s3')
bucket = s3.Bucket('mindex-data-analytics-code-challenge')
#Download Files
bucket.download_file("bengals.csv", "C:/Mindex_Coding_Challenge/bengals.csv")
bucket.download_file("boyd_receiving.csv", "C:/Mindex_Coding_Challenge/boyd_receiving.csv")
bucket.download_file("chase_receiving.csv", "C:/Mindex_Coding_Challenge/chase_receiving.csv")
bucket.download_file("higgins_receiving.csv", "C:/Mindex_Coding_Challenge/higgins_receiving.csv")
#Create Dataframes
bengals_df = pd.read_csv("C:/Mindex_Coding_Challenge/bengals.csv")
boyd_df = pd.read_csv("C:/Mindex_Coding_Challenge/boyd_receiving.csv")
chase_df = pd.read_csv("C:/Mindex_Coding_Challenge/chase_receiving.csv")
higgins_df = pd.read_csv("C:/Mindex_Coding_Challenge/higgins_receiving.csv")
#Cleansing Steps
boyd_df = boyd_df.rename(columns={'Yards': 'Boyd_Yards', 'TD':'Boyd_TD'})
chase_df = chase_df.rename(columns={'Yards':'Chase_Yards', 'TD':'Chase_TD'})
higgins_df = higgins_df.rename(columns={'Yards':'Higgins_Yards', 'TD':'Higgins_TD'})
bengals_df['Result'] = bengals_df['Result'].replace({1: 'Win', 0: 'Loss'})
#Create one dataset from all files
combined_df = pd.merge(bengals_df, boyd_df, on="Week", how = "left")
combined_df = pd.merge(combined_df, chase_df, on="Week", how="left")
combined_df = pd.merge(combined_df, higgins_df, on="Week", how="left")

#insert DF into table
host = "ls-2619b6b15c9bdc80a23f6afb7eee54cf0247da21.ca3yee6xneaj.us-east-1.rds.amazonaws.com"
engine = create_engine('postgresql://anthony_barone:bnthonyaarone@'+host+'/postgres')
combined_df.to_sql('Anthony_Barone', engine, if_exists='append', index=False)
engine.dispose()
