#!/bin/bash
sudo pip install boto3
sudo yum install wget -y
sudo mkdir /custom_scripts/
sudo wget -P /custom_scripts/ https://raw.githubusercontent.com/adammichalsky/EMR/master/scripts/parquet-conv-script-writer.py
sudo wget -P /custom_scripts/ https://raw.githubusercontent.com/adammichalsky/EMR/master/scripts/avro-schema-gen-script-writer.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/msk/mskutils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/general/listutils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/s3/s3utils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/awsutils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/ec2/ec2utils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/EMR/master/scripts/etlutils.py
sudo wget -P /custom_scripts/ https://raw.githubusercontent.com/adammichalsky/EMR/master/scripts/getTags.py
sudo wget -P /custom_scripts/ https://raw.githubusercontent.com/adammichalsky/EMR/master/scripts/parsePropertiesJSON.py
sudo chmod +x /custom_scripts/*.py
