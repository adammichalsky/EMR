#!/usr/bin/python
import s3utils, awsutils, ec2utils, mskutils, listutils, os

#Determine landing bucket and warehouse bucket
dest_bucket_base = '-bigdata-warehousing-'
src_bucket_base = '-bigdata-landing-'
dest_bucket = ec2utils.get_tag_value('Tenant Name') + dest_bucket_base + ec2utils.get_tag_value('Environment')
src_bucket = ec2utils.get_tag_value('Tenant Name') + src_bucket_base + ec2utils.get_tag_value('Environment')

#List unique folders in src bucket
dest_list =  s3utils.get_s3_folders(startAfter="",bucket=dest_bucket)
src_list = s3utils.get_s3_folders(startAfter="",bucket=src_bucket)

dest_list = listutils.deduplicate_list(dest_list)
src_list = listutils.deduplicate_list(src_list)

delta_list = listutils.diff_list(src_list,dest_list)


with open('/tmp/orc-to-parquet.py', 'w+') as file:
    file.write("#!/usr/bin/python \n")
    file.write("from __future__ import print_function \n" + "import sys \n" + "from pyspark.sql import SparkSession,functions as F \n")
    file.write("spark = SparkSession.builder.appName(\"initialization\").getOrCreate()\n\n")
    for df in range(0,len(src_list)-1):
        if src_list[df] in delta_list:
            file.write("base = spark.read.format(\"orc\").load(\"s3://" + src_bucket + "/" + src_list[df] + "/\")\n")
            file.write("newBase = base.withColumn(\"ProcessingTime\", F.current_timestamp())\n")
            if src_list[df].find('-') > -1:
                file.write("newBase.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://" + dest_bucket + "/" + src_list[df][:src_list[df].find('-')] + "\")\n\n")
            else:
                file.write("newBase.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://" + dest_bucket + "/" + src_list[df] + "\")\n\n")
    file.write("spark.stop()")


os.system("sudo chmod +x /tmp/orc-to-parquet.py")
