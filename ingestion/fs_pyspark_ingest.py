import pyspark
from pyspark.sql import SparkSession, DataFrame
import os
import sys
import argparse
import time
import boto3
import numpy as np
from botocore.config import Config


def ingest_df_into_fg(feature_group_name, rows):
    
    boto_session = boto3.Session()
    sagemaker_client = boto_session.client(service_name='sagemaker')
    featurestore_runtime = boto_session.client(service_name='sagemaker-featurestore-runtime', config=Config(
                                         retries = {
                                             'max_attempts': 10,
                                             'mode': 'standard'
                                         }))
    rows = list(rows)
    total_rows = len(rows)    
    if total_rows == 0:
        raise (f'Empty data frame')    

    for index, row in enumerate(rows):
        record = [{"FeatureName": column, "ValueAsString": str(row[column])} \
                   for column in row.__fields__ if row[column] != None]

        resp = featurestore_runtime.put_record(FeatureGroupName=feature_group_name, Record=record)

        if not resp['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise (f'PutRecord failed: {resp}')

def run_pyspark_job(args):
    spark = SparkSession.builder.appName('PySparkJob').getOrCreate()
    df = spark.read.options(Header=True).csv(args.s3_uri_prefix)
    
    if args.custom_partitions:
        df = df.repartition(args.custom_partitions)
    
    df.foreachPartition(lambda rows: ingest_df_into_fg(args.feature_group_name, rows))
    

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--custom_partitions", type=int)
    parser.add_argument("--s3_uri_prefix", type=str, required=True)
    parser.add_argument("--feature_group_name", type=str, required=True)

    args, _ = parser.parse_known_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    run_pyspark_job(args)
