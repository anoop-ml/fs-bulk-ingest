Steps (temp until the DLC images are published)

Below steps are one time only. Once public DLC images are released, there is no need for Step 1

1. Build an image from git repo of sagemaker-spark-container
2. Inherit from the above image, install [SageMaker Feature Store Spark Connector](https://aws.amazon.com/about-aws/whats-new/2022/01/amazon-sagemaker-feature-store-connector-apache-spark-batch-data-ingestion/)



Details

Step 1. Build the base [Sagemaker-spark-contaier](https://github.com/aws/sagemaker-spark-container.git).

​		Used Deep Learning AMI (Amazon Linux 2) Version 57.1 - ami-01dfbf223bd1b9835, c5.large. To avoid installing dev tools


​	   Follow the instructions at https://github.com/aws/sagemaker-spark-container/blob/master/DEVELOPMENT.md


Step 2. Run sh build_and_push.sh (this will push the final image to ECR)

