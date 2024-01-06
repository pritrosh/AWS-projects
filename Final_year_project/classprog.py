import paho.mqtt.client as paho
import os
import socket
import ssl
import time
import random 
import simplejson as json
import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import awscon
from busData import bus,hopdata,vehicle

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Bus')
connflag = False
validBus = False
dynamoflag=False
s = socket.socket() 
t1=[] 

class myClass:
	
	def __init__(self):
		print("init function")			
		
		
	def fetch(self):
		
		for i1 in bus:
			h=hopdata[i1]
			response = table.query(
			IndexName='Bus_number-index',
			KeyConditionExpression=Key('Bus_number').eq(i1)
			)
			itemCount =response['Items']
			
			for i in itemCount:
				print(" ")
				p1=int(json.dumps(i['Hop']))
				if p1<h-2:
					print("2 stops")
					print(p1)
					print(h-2)
					print(i1)
					break
					
				if p1<h-4:
					print("2 stops")
					print(h-4)
					print(i1)
					break
						
				if p1<h-6:
					print("2 stops")
					print(h-6)
					print(i1)
					break
				
				
				
					
					#print(json.dumps(i['Bus_number']),json.dumps(i['Vehicle_number']))#,i['Hop'],i['Arrival_time']) 
					print("in fetchh file")
				else:
					print("no match ff")
			
	
		
	def server_program(self):
		global dynamoflag
		port = 8894  
		server_socket = socket.socket()  
		server_socket.bind(('0.0.0.0', port))  
		global vehicle_no,busNum,hop,route
		
		
		server_socket.listen(2)
		conn, address = server_socket.accept()  
		print("Connection from: " + str(address))
		while True:
			
			busNum= conn.recv(1024)
			route= conn.recv(1024)
			vehicle_no= conn.recv(1024)
			hop= conn.recv(1024)
			dynamoflag=True
			print(vehicle)
			
			if len(busNum) ==0:
				break
			else:
				print("from connected user: " )
				i= datetime.datetime.now()
				arrival_time=i.strftime("%H:%M:%S")
				
				if(dynamoflag==True):
					table.put_item(
					Item={
						"Bus_number":busNum,
						"Route":route,
						"Vehicle_number": vehicle_no,
						 "Arrival_time": arrival_time,
						 "Hop":15,
						 "bStop_id":'10'
										 
						
						
					}
					)
					print("uploaded")
					dynamoflag=False

				print("msg sent: Data uploaded in DynamoDB") 
				
	
				
				
			
		conn.close()  
		
	
			
		
		
		
p=myClass()
while True:
	time.sleep(2)
	p.server_program()
	time.sleep(5)
	print("calling fetch")
	p.fetch()
	
	
	
	
