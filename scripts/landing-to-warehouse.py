import boto3,re,os

def get_session(region):
    return boto3.session.Session(region_name=region)

def diff(first, second):
        second = set(second)
        return [item for item in first if item[:item.find('-')] not in second]

def get_s3_tables(**kwargs):
    tables = []
    resp = s3.list_objects_v2(Bucket=kwargs['bucket'],Delimiter='/',MaxKeys=10,StartAfter=kwargs['startAfter'])
    if resp['KeyCount'] > 0:
        resp = resp['CommonPrefixes']
        if len(resp)>1:
            for obj in resp:
                tables.append(re.sub('(\/.+)|(\/)','',obj['Prefix']))
            sub_table = get_s3_tables(startAfter=tables[len(tables)-1], bucket=kwargs['bucket'])
            tables = tables + sub_table
        else:
            for obj in resp:
                tables.append(re.sub('(\/.+)|(\/)','',obj['Prefix']))
        return tables
    else:
        return tables

#Get AWS Session & client
session = get_session('us-east-1')
s3 = session.client('s3')

#Determine landing bucket and warehouse bucket
warehouse_bucket_re = 'bigdata-warehousing-(.{2,3})'
landing_bucket_re = 'big-data-landing'
warehouseBucket = ""
landingBucket = ""

#Find warehousing BUCKET and landing BUCKET in current env
for obj in s3.list_buckets()['Buckets']:
    if bool(re.search(warehouse_bucket_re,obj['Name'])):
        warehouseBucket=obj['Name']
    elif bool(re.search(landing_bucket_re,obj['Name'])):
        landingBucket=obj['Name']

#List unique folders in landing bucket
warehouseList =  get_s3_tables(startAfter="",bucket=warehouseBucket)
landingList = get_s3_tables(startAfter="",bucket=landingBucket)
s3List=[]
for i in warehouseList:
  if i not in s3List:
    s3List.append(i)

warehouseList = s3List

newFolders = diff(landingList,warehouseList)

with open('/tmp/orc-to-parquet.py', 'w+') as file:
    file.write("from __future__ import print_function \n" + "import sys \n" + "from pyspark.sql import SparkSession,functions as F \n")
    file.write("spark = SparkSession.builder.appName(\"initialization\").getOrCreate()\n")
    for df in range(0,len(landingList)-1):
        if landingList[df] in newFolders:
            file.write("base = spark.read.format(\"orc\").load(\"s3://" + landingBucket + "/" + landingList[df] + "/\")\n")
            file.write("newBase = base.withColumn(\"ProcessingTime\", F.current_timestamp())\n")
            if landingList[df].find('-') > -1:
                file.write("newBase.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://" + warehouseBucket + "/" + landingList[df][:landingList[df].find('-')] + "\")\n")
            else:
                file.write("newBase.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://" + warehouseBucket + "/" + landingList[df] + "\")\n")


os.system("sudo chmod +x /tmp/orc-to-parquet.py")
os.system("sudo /usr/lib/spark/bin/spark-submit /tmp/orc-to-parquet.py")
os.system("sudo rm -f /tmp/orc-to-parquet.py")
