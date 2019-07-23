#!/bin/bash
sudo pip install boto3
sudo yum install wget -y
sudo mkdir /custom_scripts/
sudo wget -P /custom_scripts/ https://raw.githubusercontent.com/adammichalsky/EMR/master/scripts/landing-to-warehouse.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/msk/mskutils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/general/listutils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/s3/s3utils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/awsutils.py
sudo wget -P /usr/lib/python2.7/site-packages/ https://raw.githubusercontent.com/adammichalsky/Data-IaC/master/aws/ec2/ec2utils.py
sudo chmod +x /custom_scripts/landing-to-warehouse.py
