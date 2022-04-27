
START_NOTEBOOK=${1:-false}
TIMEOUT=${2:-3600}
STEPS=${3:-s3_hdfs_copy_step.json}

CMD=~/Documents/databricks_spark_certification/aws_emr/

echo "changing to dir containing config files $CMD"
cd ~/Documents/databricks_spark_certification/aws_emr/ || exit

printf "\n Running emr create cluster command: \n"

# -1H for UTC time which what create cluster time is recorded in
CREATION_TIME=$(echo $(date -v "-1H" "+%Y-%m-%dT%H:%M:%S"))


check_cluster_status()
{
  STATUS=$(aws emr list-clusters --created-after "$CREATION_TIME" --query 'Clusters[].Status.State|[0]') || exit
  STATUS=$(echo "$STATUS"|xargs)   # remove quotes on 'WAITING' to allow str comparison
}

get_cluster_id()
#query filters by ID key in clusters array and parses first item in list
{
CLUSTER_ID=$(aws emr list-clusters --cluster-states WAITING --query 'Clusters[].Id|[0]') || exit
CLUSTER_ID=$(echo "$CLUSTER_ID"|xargs)
}


if [[ -z $STEPS ]];
then
    aws emr create-cluster \
    --release-label emr-6.1.1 \
     --service-role EMR_DefaultRole \
    --applications Name=Hadoop Name=Hive Name=Pig Name=Spark \
    --ec2-attributes file://ec2-attributes.json \
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
    --ec2-attributes file://ec2-attributes.json \
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


if [[ ${START_NOTEBOOK} = true ]];
then
  check_cluster_status
  echo ""
  # Cluster status 'STARTING' (provisioning nodes_ -> 'RUNNING' (running steps) -> 'WAITING' (ready)
  echo "Checking cluster status is in 'WAITING' state before notebook start execution ...."
  echo""
  while [[ ${STATUS} != "WAITING" ]];
  do
    echo "Cluster status still in ${STATUS} state. Waiting for a minute, before checking again"
    sleep 60
    check_cluster_status
  done

  if [[ $? -eq 0 ]];
    then
      echo ""
      echo "CLuster status now in WAITING state, so starting notebook execution"
      get_cluster_id
      aws emr --region us-east-1 \
      start-notebook-execution \
      --editor-id e-1VLA7UDB2TM65N23MXOLDAA48 \
      --relative-path parking_ticket_violations.ipynb \
      --notebook-execution-name test \
      --execution-engine Id="${CLUSTER_ID}" \
      --service-role EMR_Notebooks_DefaultRole
  fi
fi

echo "Script complete !"