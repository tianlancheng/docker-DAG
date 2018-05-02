# -*- coding: utf-8 -*-
import docker
from config import Config
import redis
import json
import datetime
from bson.objectid import ObjectId
import time

#connect to redis
pool = redis.ConnectionPool(host=Config.REDIS_HOST,port=Config.REDIS_PORT,db=0)
rcon = redis.StrictRedis(connection_pool=pool)

from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.dagdb

dockerClient=docker.DockerClient(base_url='unix://var/run/docker.sock', version=Config.DOCKER_VERSION)

def start_action(workflowId,action,inputs):
	restart=docker.types.RestartPolicy(condition='none', delay=0, max_attempts=0, window=0)
	savepath='/nfs/'+workflowId+'/'+action['name']
	dockerClient.services.create(image=action['name'].lower(), 
		name=workflowId+'-'+action['name'],
		mounts=["nfs-volume:/nfs:rw"],
		command="./run "+savepath+' '+inputs,
		restart_policy=restart,
		labels={'task':'action',"workflowId":workflowId,"actionName":action['name']})
	
# task={
# 	"type":"",
# 	"workflowId":"",
# 	"actionName":"",
# 	"error":"",
# 	"executeTime":""
# }
def execute_task(task):
	workflowId = task['workflowId']
	workflow = db.workflows.find_one({'_id':ObjectId(workflowId)})

	if not workflow:
		return
	isStop = workflow['isStop']
	if task['type'] == 'start' and not isStop:
		actionName = workflow['start']
		nextAction = workflow['actions'][actionName]
		start_action(workflowId,nextAction,"/nfs/"+workflowId+"/input")
		workflow['state'] = 'running'
		workflow['startTime'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
		workflow['actions'][actionName]['state'] = 'running'
		db.workflows.save(workflow)
	elif task['type'] == 'action':
		actionName = task['actionName']
		error = task.get('error')
		if error:
			workflow['actions'][actionName]['state'] = 'error'
			workflow['state'] = 'error'
			workflow['error'] = error
			db.workflows.save(workflow)
			return

		workflow['actions'][actionName]['state'] = 'finnish'
		workflow['actions'][actionName]['executeTime'] = task['executeTime']
		if isStop:
			workflow['actions'][actionName]['state'] = 'stoped'
			db.workflows.save(workflow)
			return

		#start next action
		nextActions = workflow['actions'][actionName]['nextActions']
		for nextActionName in nextActions:
			if nextActionName == 'end':
				workflow['state'] = 'finish'
				workflow['endTime'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
				db.workflows.save(workflow)
				return

			nextAction = workflow['actions'][nextActionName]
			if nextAction['type'] == 'action':
				start_action(workflowId,nextAction,'/nfs/'+workflowId+'/'+actionName)
				workflow['actions'][nextActionName]['state'] = 'running'

			elif nextAction['type'] == 'join':
				waitNum = nextAction['waitNum']-1
				workflow['actions'][nextActionName]['waitNum'] = waitNum
				if waitNum == 0:
					to = nextAction['to']
					if to == 'end':
						workflow['state'] = 'finish'
						workflow['endTime'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
						db.workflows.save(workflow)
						return
					toAction=workflow['actions'][to]
					if toAction['type'] == 'action':
						inputs=''
						for name in nextAction['preActions']:
							inputs=inputs+'/nfs/'+workflowId+'/'+name+' '
						start_action(workflowId,toAction,inputs)
						workflow['actions'][to]['state']='running'
					else:
						print('dag format error!')
		db.workflows.save(workflow)	

def listen_task():
	while True:
		print("waiting task...")
		task = rcon.brpop('task:queue', 0)[1]
		task=json.loads(task)
		print "get task:",task
		execute_task(task)		
		time.sleep(0.1);
		print

if __name__ == '__main__':
	print 'listen task queue'
	listen_task()

