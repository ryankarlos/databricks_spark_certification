
### Creating the EMR cluster

This assumes you have data in S3 bucket and want to copy into hdfs as a step
when creating the EMR cluster (based on s3_hdfs_copy_step.json). This enables 
s3-transfer acceleration by using `s3-accelerate.amazonaws.com` endpoint (which assumes
the respective S3 bucket has transfer acceleartion enabled (refer to `S3 uploads for 
large datasets` section).  To disable this, modify to `--s3Endpoint=s3.amazonaws.com` 
in json. Run the bash script below and pass in first arg which is desired timeout threhsold 
in secs after which the cluster auto-terminates if idle and second arg is the name of the
EC2 key-pair (set this up on AWS console if not previously) and store private key on 
local machine.

```
 databricks_spark_certification $ sh aws_emr/create_cluster.sh 5000 ec2-default

{
    "ClusterId": "j-8KHU17PIRVI0",
    "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:376337229415:cluster/j-8KHU17PIRVI0"
}

```

or to run without steps - pass empty string to second arg

```
 databricks_spark_certification $ sh aws_emr/create_cluster.sh 5000 ""
```

### ssh into master node

Once cluster has been setup - we can ssh into master node by following command.
we will need to first change permissions on file to allow only read-only access. Otherwise you will get 
an error like: `Permissions 0644 for 'youramazon.pem' are too open. It is recommended that your private key files are NOT accessible by others.
This private key will be ignored.`


Then run the ssh command with path to where you private key is stored and the 
dns of the master node (which you can get from the cluster summary on console or via cli)

```
$ chmod 400 <PRIVATE-KEY>
$ ssh -i <PATH-TO-PRIVATE-KEY> hadoop@<MASTER-PUBLIC_DNS>



Last login: Tue Apr 26 00:16:33 2022

       __|  __|_  )
       _|  (     /   Amazon Linux 2 AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-2/
42 package(s) needed for security, out of 80 available
Run "sudo yum update" to apply all updates.
                                                                    
EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR    
E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R   
EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R 
  E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
  E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
  E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R 
  E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR   
  E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R  
  E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
  E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR
                                                                    
[hadoop@ip-10-0-0-58 ~]$ 

```

The command above creates an EMR cluster with spark and configuration for
master and core nodes as speciifed in `instancegroupconfig.json` along with auto scaling
limits. The VPC subnet and availability zones are speciifed in `--ec2-attributes` values.
Finally, we set `--no-termination-protected` to allow manual termination of cluster
and set `--auto-termination-policy` to just over 1.5 hrs so the cluster terminates automatically
if in idle state for this time.
For more options and variations of this command please refer to the AWS docs here: 
https://docs.aws.amazon.com/cli/latest/reference/emr/create-cluster.html


## Monitoring the cluster metrics 

To montior cluster metrics in clouydwatch during usage, refer to https://docs.aws.amazon.com/emr/latest/ManagementGuide/UsingEMR_ViewingMetrics.html

In cloudwatch -> all metrics -> Emr -> jobflowmetrics -> filter by cluster id e.g. j-B857LVXYQNH5-> tick the corresponding metrics  and see graph being updated. You can see the selected metrics in graph metrics tab lor run queires to get further insights

<img src="https://github.com/ryankarlos/databricks_spark_certification/blob/master/screenshots/aws_emr_cluster_mertics_.png">

<img src="https://github.com/ryankarlos/databricks_spark_certification/blob/master/screenshots/Memory_cluster_EMR.png">

### S3 uploads for large datasets

S3 Transfer Acceleration helps you fully utilize your bandwidth, minimize the effect of distance on throughput, and is designed to ensure 
consistently fast data transfer to Amazon S3 regardless of your source’s location. The amount of acceleration primarily depends on your 
available bandwidth, the distance between the source and destination, and packet loss rates on the network path. Generally, you will see 
more acceleration when the source is farther from the destination, when there is more available bandwidth, and/or when the object size is bigger.

Therefore, in order to test it, one could use the Amazon S3 Transfer Acceleration Speed Comparison tool to compare accelerated 
and non-accelerated upload speeds across Amazon S3 regions. The Speed Comparison tool uses multipart uploads to transfer a file from your browser 
to various Amazon S3 regions with and without using Transfer Acceleration. You can access the Speed Comparison tool using the 
link: https://s3-accelerate-speedtest.s3-accelerate.amazonaws.com/en/accelerate-speed-comparsion.html 

<img height="500" width="800" src="https://github.com/ryankarlos/databricks_spark_certification/blob/master/screenshots/s3-transfer-acceleration-test.png">

Alternatively, for testing the upload speed of Amazon S3 Transfer Acceleration for a specific file size, follow the instructions here:
https://aws.amazon.com/premiumsupport/knowledge-center/upload-speed-s3-transfer-acceleration/  to install the required dependencies from linux command line.
and then execute the bash script downloaded from https://github.com/awslabs/aws-support-tools/blob/master/S3/S3_Transfer_Acceleration/Bash-script/test-upload.sh
Pass in required file path and s3 destination file name in the subsequent input prompts as below

```
$ sh aws_etl/datasets/test-upload.sh 

Enter the local path of the file you want to upload: /Users/rk1103/Documents/databricks_spark_certification/datasets/parking_violations/parking_violations_2017.csv
Enter the destination file name: parking_violations_2017.csv
Enter the name of the bucket: big-datasets-over-gb
Enter the region of your Bucket (e.g. us-west-2): us-east-1
Do you want to upload via Transfer Acceleration: (y/n) y
Completed 1.1 GiB/1.9 GiB (2.2 MiB/s) with 1 file(s) remaining    

```

If S3 transfer acceleration is improving results then use https://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration-examples.html#transfer-acceleration-examples-aws-cli-2
upload data to S3 bucket using transfer acceleration endpoint and check if you are getting expected results.

```
aws s3 cp ~/Downloads/archive/parking_violations_2017.csv s3://big-datasets-over-gb/parking_violations/2017/ --endpoint-url https://s3-accelerate.amazonaws.com 

Completed 532.2 MiB/1.9 GiB (1.1 MiB/s) with 1 file(s) remaining  
```

Further, if transfer acceleration is not giving improved speed for your region, then you can use S3 Multipart uploads to upload on object to S3 bucket. 
Multipart upload allows you to upload a single object as a set of parts. In general, we recommend that, when your object size reaches 100 MB, you should consider 
using multipart uploads instead of uploading the object in a single operation.  
https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
https://aws.amazon.com/premiumsupport/knowledge-center/s3-multipart-upload-cli/


So, you can make use of AWS CLI in order to upload objects using multipart upload. Also, kindly know that, by default 'multipart_threshold' value is set to 8MB so, 
when uploading, downloading, or copying a file,  S3 will automatically switch to multipart operations if the file reaches a given size threshold. However, in order to 
improve upload performance  you can customize the values for multipart upload configuration i.e the values for multipart_threshold,  multipart_chunk size etc. 
accordingly, in the default location of ~/.aws/config file.  So, the chunk size can be increased in the CLI in the ~/.aws/config file so that you have bigger chunk sizes and therefore less parts. Please refer the below document for more information:
https://docs.aws.amazon.com/cli/latest/topic/s3-config.html

```
$ aws configure set default.s3.multipart_chunksize 1GB
$ aws s3 cp ~/Documents/databricks_spark_certification/datasets/seattle_library/library-collection-inventory.csv s3://big-datasets-over-gb/seattle_library/ \
--endpoint-url https://s3-accelerate.amazonaws.com

Completed 328.2 MiB/11.0 GiB (2.2 MiB/s) with 1 file(s) remaining  
```

<b> Note: upload speed depends on many factors such as internet speed, file size and concurrency including network slowness or congestion from client’s end etc.
