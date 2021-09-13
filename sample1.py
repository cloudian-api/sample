# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 11:42:19 2020

@author: billschreckenstein
"""

import boto3
import requests

class my(object):

	# Global declaration of access key for connection to object store
	global accessKey
	accessKey = "00e069e528883479501a"

	# Global declaration of secret key for connection to object store
	global secretKey
	secretKey = "P07un2Pan91Dn4iUcs/wSYxt/zmxwyV6Ni/1DcYw"

	# Global declaration of HyperStore endpoint
	global endPoint
	endPoint = "http://s3-us.hyperstore.local"

	# Bucket name used for certain examples globally in code
	global bucketId
	bucketId = "metadata"

	# Global variable to hold s3 resource
	global s3

	# Global variable to hold primary bucket for operations
	global targetBucket

	# Global variable used to indicate bucket discovery state
	global exists

	# Client variable used for specific methods
	global client

	#
	# Method: __init__
	# Description: init method used to self launch application
	#
	def __init__(self):
		self.spawnConnection();
		self.uploadObjectWithMetadata();
		self.uploadObjectNoMetadata();
		self.listBucket();
		self.getObject();
		self.rangeRead();

	#
	# Method: spawnConnection
	# Description: Method used to connect to HyperStore and establish our targetBucket
	# for future application actions.
	#
	def spawnConnection(self):
		# Define connection to HyperStore resource
		self.s3 = boto3.resource('s3', aws_access_key_id=accessKey, aws_secret_access_key=secretKey, endpoint_url=endPoint)
		# Create targetBucket variable for future application actions
		self.targetBucket = self.s3.Bucket(bucketId)
		# Assume bucket exists unless an exception is propagated
		self.exists = True
		try:
			# Attempt head bucket call to ensure named bucket exists
			self.s3.meta.client.head_bucket(Bucket=bucketId)
		# Handle exception and note bucket as non-existent and tell user
		except botocore.exceptions.ClientError as e:
			error_code = int(e.response['Error']['Code'])
			if error_code == 404:
				self.exists = False
				print("Bucket Does Not Exist!")
	#
	# Method: uploadObjectNoMetadata
	# Description: uploads object without metadata to HyperStore
	#
	def uploadObjectNoMetadata(self):
		if self.exists:
			fileName = "images/car1.jpg"
			keyName = "mustang"
			self.targetBucket.upload_file(fileName, keyName)
	#
	# Method: uploadObjectWithMetadata
	# Description: uploads object with metadata to HyperStore
	#
	def uploadObjectWithMetadata(self):
		if self.exists:
			fileName = "images/car2.jpg"
			keyName = "bmw"
			extra = {"Metadata": {'x-amz-make':'bmw','x-amz-year':'1973','x-amz-color':'red','x-amz-type':'turbocharged'}}
			self.targetBucket.upload_file(fileName, keyName, extra)

	#
	# Method: listBucket
	# Description: method will grab list of objects from bucket and iterate
	# over the object list and print possible data parameters
	#
	def listBucket(self):
		# If the bucket exists grab the list of objects
		if self.exists:
			output = self.targetBucket.objects.all()
			# iterator used to identify each object
			iterator = iter(output)

			# loop over iterator and check for certain fields and print the output if fields are populated
			for key in iterator:
				# extract each key one at a time
				obj_str = key.key
				tempObj = self.s3.Object(bucket_name=self.targetBucket.name,key=key.key)

				# is the object verisioned and have a version id field, if so print it
				if tempObj.version_id:
					print(tempObj.version_id)

				# is the object locked and have a retention date assigned, if so print it
				if tempObj.object_lock_retain_until_date:
					print(str(tempObj.object_lock_retain_until_date))

				# does the object have metadata, if so print the metadata
				if tempObj.metadata:
					print(str(tempObj.metadata).replace('{','').replace('}',''))

				# how big is the object, print the content length
				if tempObj.content_length:
					print(str(tempObj.content_length))

				# check last modified data for object and print it
				if tempObj.last_modified:
					print(str(tempObj.last_modified))

	#
	# Method: getObject
	# Description: method used to get an object and write it to a local file called sample.object
	#
	def getObject(self):
		# Establish client connection
		client = boto3.client('s3', aws_access_key_id = accessKey, aws_secret_access_key = secretKey, endpoint_url = endPoint)
		# Grab bmw object and stream the body to a file
		resp = client.get_object(Bucket=bucketId,Key="bmw")
		data = resp['Body'].read()

		# Open and write sample.object file with data stream
		aFile = open("sample.object", "wb")
		aFile.write(data)
		aFile.close()

	#
	# Method: rangeRead
	# Description: method used to read first 16 bytes of an object and write it to a file called slice.object
	#
	def rangeRead(self):
		# Establish client connection
		client = boto3.client('s3', aws_access_key_id = accessKey, aws_secret_access_key = secretKey, endpoint_url = endPoint)
		# Specify byte range and read first 16 bytes of the object
		Bytes_range = 'bytes=0-15'
		resp = client.get_object(Bucket=bucketId,Key="bmw",Range=Bytes_range)
		data = resp['Body'].read()

		# Open and write slice.object file with data stream
		aFile = open("slice.object", "wb")
		aFile.write(data)
		aFile.close()

myObj = my()
