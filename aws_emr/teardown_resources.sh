set -e
# crete list var from output of command
cluster_id_list=($(aws emr list-clusters --cluster-states WAITING --query 'Clusters[].Id'))

echo ""

# for some reason empty list condition check does not work if -eq 0
# so need to set to -eq 1
if [[ "${#cluster_id_list[*]}" -eq 1 ]];
then
  echo "No clusters to be deleted."
else
  for i in "${cluster_id_list[@]}";
  do
    if [[ $i =~ j-* ]];
    then
      cluster_id=$(echo $i | xargs)
      echo "Deleting cluster with id: $cluster_id"
      echo "Running command: 'emr terminate-clusters --cluster-id ${cluster_id}' ...."
      aws emr terminate-clusters --cluster-ids "${cluster_id}"
    fi;
  done
fi;

echo "Done"