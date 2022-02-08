ARG SM_PYSPARK_ECR_ID
ARG REGION

FROM ${SM_PYSPARK_ECR_ID}.dkr.ecr.${REGION}.amazonaws.com/sagemaker-spark-processing:3.0-cpu

RUN /usr/bin/python3 -m pip install sagemaker-feature-store-pyspark --no-binary :all: --verbose