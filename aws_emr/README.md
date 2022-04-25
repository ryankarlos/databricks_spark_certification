## S3 uploads for large datasets

S3 Transfer Acceleration helps you fully utilize your bandwidth, minimize the effect of distance on throughput, and is designed to ensure 
consistently fast data transfer to Amazon S3 regardless of your source’s location. The amount of acceleration primarily depends on your 
available bandwidth, the distance between the source and destination, and packet loss rates on the network path. Generally, you will see 
more acceleration when the source is farther from the destination, when there is more available bandwidth, and/or when the object size is bigger.

Therefore, in order to test it, one could use the Amazon S3 Transfer Acceleration Speed Comparison tool to compare accelerated 
and non-accelerated upload speeds across Amazon S3 regions. The Speed Comparison tool uses multipart uploads to transfer a file from your browser 
to various Amazon S3 regions with and without using Transfer Acceleration. You can access the Speed Comparison tool using the 
link: https://s3-accelerate-speedtest.s3-accelerate.amazonaws.com/en/accelerate-speed-comparsion.html 

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

