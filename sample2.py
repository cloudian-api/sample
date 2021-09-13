# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 11:42:19 2020

@author: billschreckenstein
"""

import boto3
import requests
import urllib3

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#
# Details using python to communicate with admin apiPort
#

class apiHandler(object):

    #
    # Global variable section
    #

    global adminName
    adminName = "sysadmin"
    global adminPass
    adminPass = "R5QMy+VfWf/dj+Xb7kz6cKMWnvg="
    global host
    host = "192.168.49.100"
    global apiPort
    apiPort = "19443"

	#
	# Method: __init__
	# Description: init method used to self launch application
	#
    def __init__(self):
        self.readPoliciesAndReturnSelection();
        self.generateReport();
        self.monitorOutput();

    #
    # Method that connects to HyperStore and lists all the available storage policies
    #
    def readPoliciesAndReturnSelection(self):
        #
        # Prepare and issue request string
        #
        response = requests.get('https://' + host + ':' + str(apiPort) + '/bppolicy/listpolicy', verify=False, auth=(adminName, adminPass))

        output = {}
        output = response.json()
        iterator = iter(output)
        tag = 0

        #
        # Iterate through policy list and print fields
        #
        for policy in iterator:
            tag = output.index(policy)
            option = str(tag) + ".) " + policy['policyId'] + "\t" + policy['policyName']
            print(option)
            print("\t" + policy['policyDescription'])

    #
    # Will calculate capacity used by an array of buckets passed to this method.
    #
    def processPolicyBuckets(self, buckets):
        iterator = iter(buckets)
        total = 0.0
        for bucket in iterator:
            response = requests.post('https://' + host + ':' + str(apiPort) + '/usage/repair/bucket?bucket=' + bucket, verify=False, auth=(adminName, adminPass))
            print("Tabulating bucket: " + bucket)
            output = {}
            output = response.json()
            total = total + output['TB']
        total = total / (1000 * 1000 * 1000 * 1000)
        print(str(total) + ' TBs')

    #
    # Method that generates the list of policies and the amount of storage attributed to them
    #
    def generateReport(self):
        response = requests.get('https://' + host + ':' + str(apiPort) + '/bppolicy/listpolicy', verify=False, auth=(adminName, adminPass))

        output = {}
        output = response.json()
        iterator = iter(output)
        tag = 0
        for policy in iterator:
            policyId = policy['policyId']
            print(policyId)

        response = requests.get('https://' + host + ':' + str(apiPort) + '/bppolicy/bucketsperpolicy', verify=False, auth=(adminName, adminPass))
        output = response.json()
        iterator = iter(output)
        for buckets in iterator:
            print (buckets['policyName'])
            bucketIds = buckets['buckets']
            self.processPolicyBuckets(bucketIds)

    #
    # Method used to dump node metrics on a per host basis in json format
    #
    def monitorOutput(self):
        response = requests.get('https://' + host + ':' + str(apiPort) + '/monitor/host?nodeId=hyperstore1', verify=False, auth=(adminName, adminPass))

        output = {}
        output = response.json()

        print(output)

apiObj =  apiHandler()
