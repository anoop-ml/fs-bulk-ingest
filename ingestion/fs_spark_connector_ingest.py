import os
import sys
import argparse
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
import feature_store_pyspark

def run_pyspark_job(args):
    
    extra_jars = ",".join(feature_store_pyspark.classpath_jars())
    spark = SparkSession.builder \
                        .config("spark.jars", extra_jars) \
                        .getOrCreate()
    
    df = spark.read.options(Header=True).csv(args.s3_uri_prefix)
    
    feature_store_manager= FeatureStoreManager()
    
    feature_store_manager.ingest_data(input_data_frame=df, feature_group_arn=args.feature_group_arn)

    
    print ('done ingesting')
    

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_uri_prefix", type=str, required=True)
    parser.add_argument("--feature_group_arn", type=str, required=True)

    args, _ = parser.parse_known_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    run_pyspark_job(args)
