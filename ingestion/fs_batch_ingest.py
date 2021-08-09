import pandas as pd
import os
import glob
import subprocess
import sys
import argparse

subprocess.check_call([sys.executable, "-m", "pip", "install", "sagemaker"])
import sagemaker as sm

from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session

sm_session = Session()       

def cast_object_to_string(df):
    for col in df.columns:
        if df.dtypes[col] == 'object':
            df[col] = df[col].astype('str')


def ingest_data(args):   

    file_list = glob.glob('/opt/ml/processing/input/*.csv')
    df = pd.concat([pd.read_csv(f) for f in file_list], ignore_index=True)
    cast_object_to_string(df)

    fg = FeatureGroup(name=args.feature_group_name, sagemaker_session=sm_session)
    resp = fg.ingest(data_frame=df, max_processes=args.num_processes, max_workers=args.num_workers, wait=True)
    
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_processes", type=int, default=1)
    parser.add_argument("--num_workers", type=int, default=1)
    parser.add_argument("--feature_group_name", type=str, required=True)

    args, _ = parser.parse_known_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    ingest_data(args)
