#!/usr/bin/python
import sys, ec2utils, json

def numberOfProcesses(list):
    return len(list['ETL-PROCESSES'])

def numberOfSources(list, processIndex):
    list = getSourcesList(list, processIndex)
    return len(list)

def getSourceJSONValues(list, keyTag, index, processIndex):
    list = getSourcesList(list, processIndex)
    switcher ={
        "SOURCE_BUCKET_NAME": list[int(index)]['SOURCE_BUCKET_NAME'],
        "SOURCE_PREFIX":  list[int(index)]['SOURCE_PREFIX'],
        "SOURCE_FOLDER_NAME": list[int(index)]['SOURCE_FOLDER_NAME'],
        "SPARK_VIEW_NAME": list[int(index)]['SPARK_VIEW_NAME'],
        "SOURCE_FORMAT": list[int(index)]['FORMAT']
              }
    return switcher.get(keyTag, "Invalid key name.")

def getDestinationJSONValues(list, keyTag, processIndex):
    list = getDestinationList(list, processIndex)
    switcher ={
        "DESTINATION_FOLDER_NAME": list[int(processIndex)]['DESTINATION_FOLDER_NAME'],
        "DESTINATION_FORMAT":  list[int(processIndex)]['DESTINATION_FORMAT'],
        "DESTINATION_BUCKET_NAME": list[int(processIndex)]['DESTINATION_BUCKET_NAME'],
        "DESTINATION_PREFIX": list[int(processIndex)]['DESTINATION_PREFIX'],
        "DROP_AND_RELOAD" : list[int(processIndex)]['DROP_AND_RELOAD']
              }
    return switcher.get(keyTag, "Invalid key name.")

def getProcessJSONValues(list, keyTag, processIndex):
    list = getProcessesList(list)
    switcher ={
        "SPARK_VIEW_NAME" : list[int(processIndex)]['SPARK_VIEW_NAME'],
        "SQL" : list[int(processIndex)]['SQL'],
        "PRIORITY" : list[int(processIndex)]['PRIORITY'],
        "REGISTER_AS_VIEW" : list[int(processIndex)]['REGISTER_AS_VIEW'],
        "PERSISTENT": list[int(processIndex)]['PERSISTENT']
              }
    return switcher.get(keyTag, "Invalid key name.")


def getSourcesList(list, processIndex):
    return getProcessesList(list)[int(processIndex)]['SOURCES']

def getDestinationList(list, processIndex):
    return getProcessesList(list)[int(processIndex)]['DESTINATION_SETTINGS']

def getProcessesList(list):
    return list['ETL-PROCESSES']

def getProcessIndexByPriority(list, priority):
    index = -1
    for i in range(0, numberOfProcesses(list)):
        if(priority == getProcessJSONValues(list,'PRIORITY',i)):
            index = i
    return index
