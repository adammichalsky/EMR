#!/usr/bin/python
import sys, json, etlutils
input = sys.stdin.readlines()

#input[0] is the properties file path
#input[1] is the operation
#input[2] is the keyTag OR priority
#input[3] is the processIndex
#input[4] is the sourceIndex

input = list(map(str.rstrip,input))
filepath = input[0]
operation = input[1]




with open (input[0].rstrip(), "r") as file:
    json_data = file.read()
json_list = json.loads(json_data)

if operation=="GET_SOURCE_JSON_VALUES":
    print(etlutils.getSourceJSONValues(json_list, input[2], input[3], input[4]))
elif operation=="GET_PROCESS_JSON_VALUES":
    print(etlutils.getProcessJSONValues(json_list, input[2], input[3]))
elif operation=="GET_DESTINATION_JSON_VALUES":
    print(etlutils.getDestinationJSONValues(json_list, input[2], input[3]))
elif operation=="GET_PROCESS_INDEX_BY_PRIORITY":
    print(etlutils.getProcessIndexByPriority(json_list, input[2]))
elif operation=="NUMBER_OF_PROCESSES":
    print(etlutils.numberOfProcesses(json_list))
elif operation=="NUMBER_OF_SOURCES":
    print(etlutils.numberOfSources(json_list), input[2])
