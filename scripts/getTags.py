#!/usr/bin/python
import ec2utils, sys

tag = sys.stdin.readlines()
for i in range(0,len(tag)):
    print(ec2utils.get_tag_value(tag[i].rstrip()))
