#!/usr/bin/python
import sys, os, boto3, re, subprocess


def get_session(region):
    return boto3.session.Session(region_name=region)


def get_s3_folders(**kwargs):
    tables = []
    resp = s3.list_objects_v2(Bucket=kwargs['bucket'],Delimiter='/',MaxKeys=10,StartAfter=kwargs['startAfter'])
    if resp['KeyCount'] > 0:
        resp = resp['CommonPrefixes']
        if len(resp)>1:
            for obj in resp:
                tables.append(re.sub('(\/.+)|(\/)','',obj['Prefix']))
            sub_table = get_s3_folders(startAfter=tables[len(tables)-1], bucket=kwargs['bucket'])
            tables = tables + sub_table
        else:
            for obj in resp:
                tables.append(re.sub('(\/.+)|(\/)','',obj['Prefix']))
        return tables
    else:
        return tables



session = get_session('us-east-1')
s3 = session.client('s3')
bucket_re = 'amwater-bigdata-warehousing-(.{2,3})'
bucket = ""

#Find warehousing BUCKET
for obj in s3.list_buckets()['Buckets']:
    if bool(re.search(bucket_re,obj['Name'])):
        bucket=obj['Name']

tables =  get_s3_folders(startAfter="",bucket=bucket)


with open('/tmp/spark-avro-schema.py', 'w+') as file:
    file.write("#!/usr/bin/python \n")
    file.write("from __future__ import print_function \n" + "import sys \n" + "from pyspark.sql import SparkSession,functions as F \n")
    file.write("spark = SparkSession.builder.appName(\"schemaGen\").getOrCreate()\n\n")
    for df in range(0,len(tables)-1):
        if tables[df] != 'historicalWeather' or tables[df] != 'badgePhotos':
            file.write("with open('/tmp/"+tables[df] +"_schemas.txt','w') as file:\n\t")
            file.write("base = spark.read.format(\"parquet\").load(\"s3://" + bucket + "/" + tables[df] + "/\").limit(1)\n\t")
            file.write('header = "{\\n \\"namespace\\": \\"batch\\",\\n \\"type\\": \\"record\\",\\n \\"name\\" : \\"' + tables[df]+ '\\",\\n \\"fields\\" : [\\n"\n\t')
            file.write("schema = str(base.schema)\n\t")
            file.write("schema_list = schema.replace('StructType(List(','') \\\n\t\t")
            file.write(".replace(')))','') \\\n\t\t")
            file.write(".replace('true','') \\\n\t\t")
            file.write(".replace('false','') \\\n\t\t")
        file.write(".replace('StructField(','') \\\n\t\t")
        file.write(".replace('),','') \\\n\t\t")
        file.write(".replace(',ProcessingTime,TimestampType,','') \\\n\t\t")
        file.write(".replace('NullType','null') \\\n\t\t")
        file.write(".replace('StringType','string') \\\n\t\t")
        file.write(".replace('BooleanType','boolean') \\\n\t\t")
        file.write(".replace('DecimalType','float') \\\n\t\t")
        file.write(".replace('DoubleType','double') \\\n\t\t")
        file.write(".replace('FloatType','float') \\\n\t\t")
        file.write(".replace('ByteType','bytes') \\\n\t\t")
        file.write(".replace('IntegerType','int') \\\n\t\t")
        file.write(".replace('LongType','long') \\\n\t\t")
        file.write(".replace('DateType','string') \\\n\t\t")
        file.write(".replace('TimestampType','string') \\\n\t\t")
        file.write(".split(',') \n\n\t")
            file.write("line = ''\n\n\t")
            file.write("for i in range(0,len(schema_list),2):\n\t\t")
            file.write('line = line + "{\\"name\\": \\"" + schema_list[i] + "\\",\\"type\\": [\\"" + schema_list[i+1] + "\\",\\"null\\"]}"\n\t\t')
            file.write ('if(i+1) != len(schema_list)-1:\n\t\t\t')
            file.write('line = line + ",\\n"\n\t\t')
            file.write('else:\n\t\t\t')
            file.write('line = line + "\\n]}"\n\t\t')
            file.write('avro_schema = header + line\n\t\t')
            file.write('file.write(avro_schema + "\\n\\n")\n')
    file.write("spark.stop()\n\n")
