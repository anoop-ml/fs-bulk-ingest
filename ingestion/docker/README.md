Steps (temp until the DLC images are published)

Below steps are one time only. Once public DLC images are released, there is no need for Step 1

1. Build an image from git repo of sagemaker-spark-container
2. Inherit from the above image, install [SageMaker Feature Store Spark Connector](https://aws.amazon.com/about-aws/whats-new/2022/01/amazon-sagemaker-feature-store-connector-apache-spark-batch-data-ingestion/)



Details

1. Build the base Sagemaker-spark-contaier. Please use this fork as main is pending [merge](https://github.com/aws/sagemaker-spark-container/pull/75)

​		Used Deep Learning AMI (Amazon Linux 2) Version 57.1 - ami-01dfbf223bd1b9835, c5.large. To avoid installing dev tools

   	Git clone from private repo until the pull request is approved - https://github.com/can-sun/sagemaker-spark-container.git

​	   Follow the instructions at https://github.com/aws/sagemaker-spark-container/blob/master/DEVELOPMENT.md

​       	Need to do step 1 (python venv)

​       	Step 4 - make sure you have the right permissions

​       	Step 6 - Set environment vairables. Ignore the SAGEMAKER_ROLE

​       	make build      

2. Run sh build_and_push.sh (this will push the final image to ECR)

