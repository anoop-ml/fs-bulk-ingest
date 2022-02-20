ARG SM_PYSPARK_ECR_ID
ARG REGION

# when sagemaker-pyspark-container releases pushes new images
#FROM ${SM_PYSPARK_ECR_ID}.dkr.ecr.${REGION}.amazonaws.com/sagemaker-spark-processing:3.0-cpu
FROM sagemaker-spark:3.0-cpu-py37-v1.0

RUN /usr/bin/python3 -m pip install sagemaker-feature-store-pyspark --no-binary :all: --verbose