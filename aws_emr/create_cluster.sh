
TIMEOUT=${1}
KEYNAME=${2}
STEPS=${3:-s3_hdfs_copy_step.json}
EBS_VOL=${4:-12}

CMD=~/Documents/databricks_spark_certification/aws_emr/

echo "cd to dir containing config files $CMD"
cd ~/Documents/databricks_spark_certification/aws_emr/ || return

printf "\n Running emr create cluster command: \n"

if [[ -z $STEPS ]];
then
  aws emr create-cluster \
  --release-label emr-6.1.1 \
   --service-role EMR_DefaultRole \
  --applications Name=Hadoop Name=Hive Name=Pig Name=Spark \
  --ec2-attributes KeyName="$KEYNAME",SubnetId=subnet-0abc547ac8d132f33,\
  InstanceProfile=EMR_EC2_DefaultRole,\
  EmrManagedMasterSecurityGroup=sg-073cab3660129538a,\
  EmrManagedSlaveSecurityGroup=sg-0ddd53fcdbb613680 \
  --instance-groups file://instancegroupconfig.json \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --no-termination-protected \
  --ebs-root-volume-size "$EBS_VOL" \
  --auto-termination-policy IdleTimeout="$TIMEOUT" \
  --log-uri s3://aws-logs-376337229415-us-east-1/elasticmapreduce/ \
  --enable-debugging
else
  aws emr create-cluster \
  --steps file://"${STEPS}" \
  --release-label emr-6.1.1 \
   --service-role EMR_DefaultRole \
  --applications Name=Hadoop Name=Hive Name=Pig Name=Spark \
  --ec2-attributes KeyName="$KEYNAME",SubnetId=subnet-0abc547ac8d132f33,\
  InstanceProfile=EMR_EC2_DefaultRole,\
  EmrManagedMasterSecurityGroup=sg-073cab3660129538a,\
  EmrManagedSlaveSecurityGroup=sg-0ddd53fcdbb613680 \
  --instance-groups file://instancegroupconfig.json \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --no-termination-protected \
  --ebs-root-volume-size "$EBS_VOL" \
  --auto-termination-policy IdleTimeout="$TIMEOUT" \
  --log-uri s3://aws-logs-376337229415-us-east-1/elasticmapreduce/ \
  --enable-debugging
fi;



