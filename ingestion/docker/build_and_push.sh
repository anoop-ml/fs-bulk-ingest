# List of available images - https://github.com/aws/sagemaker-spark-container/blob/master/available_images.md

SM_PYSPARK_ECR_ID=173754725891
region=us-east-1
image=sagemaker-spark-processing
tag=3.0-cpu

# Get the account number associated with the current IAM credentials
account=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]
then
    exit 255
fi


fullname="${account}.dkr.ecr.${region}.amazonaws.com/${image}:${tag}"

# Login to docker to access the base container
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${SM_PYSPARK_ECR_ID}.dkr.ecr.${region}.amazonaws.com


# Build the container
docker build -t ${image} . --build-arg REGION=${region} --build-arg SM_PYSPARK_ECR_ID=${SM_PYSPARK_ECR_ID}
docker tag ${image} ${fullname}

# If the repository doesn't exist in ECR, create it.
aws ecr describe-repositories --region ${region} --repository-names "${image}" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "creating ECR repository : ${fullname} "
    aws ecr create-repository --region ${region} --repository-name "${image}" > /dev/null
fi


aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${account}.dkr.ecr.${region}.amazonaws.com
docker push ${fullname}
if [ $? -eq 0 ]; then
	echo "Amazon ECR URI: ${fullname}"
else
	echo "Error: Image build and push failed"
	exit 1
fi

