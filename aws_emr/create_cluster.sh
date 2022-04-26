
START_NOTEBOOK=${1:-true}
KEYNAME=${2:-ec2-default}
TIMEOUT=${3:-3600}
STEPS=${4:-s3_hdfs_copy_step.json}

CMD=~/Documents/databricks_spark_certification/aws_emr/

echo "cd to dir containing config files $CMD"
cd ~/Documents/databricks_spark_certification/aws_emr/ || return

printf "\n Running emr create cluster command: \n"

CREATION_TIME=$(echo $(date -v "-3H" "+%Y-%m-%dT%H:%M:%S"))

if [[ -z $STEPS ]];
then
  aws emr create-cluster \
  --release-label emr-6.1.1 \
   --service-role EMR_DefaultRole \
  --applications Name=Hadoop Name=Hive Name=Pig Name=Spark \
  --ec2-attributes KeyName="${KEYNAME}",SubnetId=subnet-0abc547ac8d132f33,InstanceProfile=EMR_EC2_DefaultRole,EmrManagedMasterSecurityGroup=sg-073cab3660129538a,EmrManagedSlaveSecurityGroup=sg-0ddd53fcdbb613680 \
  --instance-groups file://instancegroupconfig.json \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --no-termination-protected \
  --auto-termination-policy IdleTimeout="${TIMEOUT}" \
  --log-uri s3://aws-logs-376337229415-us-east-1/elasticmapreduce/ \
  --enable-debugging
else
  aws emr create-cluster \
  --steps file://"${STEPS}" \
  --release-label emr-6.1.1 \
   --service-role EMR_DefaultRole \
  --applications Name=Hadoop Name=Hive Name=Pig Name=Spark \
  --ec2-attributes KeyName="${KEYNAME}",SubnetId=subnet-0abc547ac8d132f33,InstanceProfile=EMR_EC2_DefaultRole,EmrManagedMasterSecurityGroup=sg-073cab3660129538a,EmrManagedSlaveSecurityGroup=sg-0ddd53fcdbb613680 \
  --instance-groups file://instancegroupconfig.json \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --no-termination-protected \
  --auto-termination-policy IdleTimeout="${TIMEOUT}" \
  --log-uri s3://aws-logs-376337229415-us-east-1/elasticmapreduce/ \
  --enable-debugging
fi;


if [ $? -eq 0 ];
then
echo ""
echo "Cluster Creation time: $CREATION_TIME"
fi;

if [[ ${START_NOTEBOOK} -eq true ]];
then
  STATUS=$(aws emr list-clusters --created-after "$CREATION_TIME" --active --query 'Clusters[].Status.State|[0]') || exit
  STATUS=$(echo $STATUS|xargs)   # remove quotes on 'WAITING' to allow str comparison
  echo ""
  echo "Checking cluster status is in 'WAITING' state before notebook start execution ...."
  echo""
  while [[ ${STATUS} != "WAITING" ]];
  do
    echo "Cluster status still on: $STATUS. Waiting for a minute, before checking again"
    sleep 60
    aws emr list-clusters --created-after="$CREATION_TIME" --active --query 'Clusters[].Status.State|[0]'| read STATUS;
  done

  if [[ $? -eq 0 ]];
  then
    echo ""
    echo "CLuster status now in WAITING state, so starting notebook execution"
    aws emr --region us-east-1 \
    start-notebook-execution \
    --editor-id e-1VLA7UDB2TM65N23MXOLDAA48 \
    --relative-path parking_ticket_violations.ipynb \
    --notebook-execution-name test \
    --execution-engine '{"Id" : "j-3UZJRU19QI2AM"}' \
    --service-role EMR_Notebooks_DefaultRole
  fi;
fi;
