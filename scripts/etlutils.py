#!/usr/bin/python
import sys, ec2utils, json

def numberOfProcesses(list):
    return len(list['ETL-PROCESSES'])

def numberOfSources(list, processIndex):
    return len(list['SOURCES'][processIndex])

def getSourceJSONValues(list, keyTag, index, processIndex):
    list = getSourcesList(list, processIndex)
    switcher ={
        "SOURCE_BUCKET_NAME": list[index]['SOURCE_BUCKET_NAME'],
        "SOURCE_PREFIX":  list[index]['SOURCE_PREFIX'],
        "SOURCE_FOLDER_NAME": list[index]['SOURCE_FOLDER_NAME'],
        "SPARK_VIEW_NAME": list[index]['SPARK_VIEW_NAME'],
        "SOURCE_FORMAT": list[index]['FORMAT']
              }
    return switcher.get(keyTag, "Invalid key name.")

def getDestinationJSONValues(list, keyTag, processIndex):
    list = getDestinationList(list, processIndex)
    switcher ={
        "DESTINATION_FOLDER_NAME": list[index]['DESTINATION_FOLDER_NAME'],
        "DESTINATION_FORMAT":  list[index]['DESTINATION_FORMAT'],
        "DESTINATION_BUCKET_NAME": list[index]['DESTINATION_BUCKET_NAME'],
        "DESTINATION_PREFIX": list[index]['DESTINATION_PREFIX'],
        "DROP_AND_RELOAD" : list[index]['DROP_AND_RELOAD']
              }
    return switcher.get(keyTag, "Invalid key name.")

def getProcessJSONValues(list, keyTag, processIndex):
    list = getProcessesList(list)
    switcher ={
        "SQL" : list[index]['SQL'],
        "PRIORITY" : list[index]['PRIORITY'],
        "REGISTER_AS_VIEW" : list[index]['REGISTER_AS_VIEW'],
        "PERSISTENT": list[index]['PERSISTENT']
              }
    return switcher.get(keyTag, "Invalid key name.")


def getSourcesList(list, processIndex):
    return getProcessesList(list)[processIndex]['SOURCES']

def getDestinationList(list, processIndex):
    return getProcessesList(list)[processIndex]['DESTINATION_SETTINGS']

def getProcessesList(list):
    return list['ETL-PROCESSES']

def getProcessIndexByPriority(list, priority):
    index = -1
    for i in range(0, numberOfProcesses(list)):
        if(priority == getProcessJSONValues(getProcessesList(list), i, 'PRIORITY')):
            index = i
    return index
