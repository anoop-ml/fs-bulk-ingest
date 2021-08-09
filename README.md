# Sample to bulk ingest data into SageMaker Feature Store



## Intro

The sample shows how to generate and ingest a csv file containing 10 millions rows, chunked into 10 files, each containing 1 million rows into [SageMaker Feature Store](https://aws.amazon.com/sagemaker/feature-store/) using [SageMaker Processing Jobs](https://docs.aws.amazon.com/sagemaker/latest/dg/use-scikit-learn-processing-container.html). A [SageMaker Notebook instance](https://docs.aws.amazon.com/sagemaker/latest/dg/nbi.html) of type ml.c5.4xlarge was used to load/run the notebooks in this repo



## Prerequisites

Please make sure the role attached to the notebook instance has the below policies. For this sample the role has

- [AmazonSageMakerFullAccess](https://github.com/awsdocs/amazon-sagemaker-developer-guide/blob/master/doc_source/sagemaker-roles.md)
- [AmazonSageMakerFeatureStoreAccess](https://docs.aws.amazon.com/sagemaker/latest/dg/feature-store-adding-policies.html)
- AmazonS3FullAccess (limit the permissions to specific buckets)



## Sample Notebooks

- [Data Generation](data-prep/dataset_generation.ipynb) - To generate sample data from seed.csv, chunk and save them to S3
- [Feature Group Create/Provision](ingestion/create_feature_group.ipynb) - Creates the feature group you want to use
- [Ingest using Ingest API in Python SDK ](ingestion/sm_processing.ipynb)
- [Ingest using PutRecord API in PySpark ](ingestion/sm_processing_pyspark.ipynb)
- [Validation](ingestion/validate_ingest.ipynb) - Read from feature store to validate (a small sample of the dataset)

