import docker
from config import Config
import redis
import json
import datetime

#connect to redis
pool = redis.ConnectionPool(host=Config.REDIS_HOST,port=Config.REDIS_PORT,db=0)
rcon = redis.StrictRedis(connection_pool=pool)

from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.dagdb

dockerClient=docker.DockerClient(base_url='unix://var/run/docker.sock', version=Config.DOCKER_VERSION)

def start_action(workflow_id,action):
	dockerClient.services.run(image=action['name'], name=action['name'],labels={'task':'action'})
	
# task={
# 	"type":"",
# 	"workflow_id":"",
# 	"action_name":"",
# 	"error":"",
# 	"execute_time":""
# }
def execute_task(task):
	workflow_id = task['workflow_id']
	workflow = db.worflows.find_one({'_id':ObjectId(workflow_id)})
	if not workflow:
		return
	isStop = workflow['isStop']
	if task['type'] == 'start' and not isStop:
		start_name = workflow['start']
		next_action = workflow['actions'][start_name]
		start_action(workflow_id,next_action)
		workflow['state'] = 'running'
		workflow['start_time'] = datetime.datetime.now()
		workflow['actions'][action_name]['state'] = 'running'
		db.worflows.save(workflow)
	elif task['type'] == 'action':
		action_name = task['action_name']
		error = task.get('error')
		if error:
			workflow['actions'][action_name]['state'] = 'error'
			workflow['state'] = 'error'
			db.worflows.save(workflow)
			return

		workflow['actions'][action_name]['state'] = 'finnish'
		workflow['actions'][action_name]['execute_time'] = task['execute_time']
		if isStop:
			db.worflows.save(workflow)
			return
		#start next action
		ok_to = workflow['actions'][action_name]['ok_to']
		if ok_to == 'end':
			workflow['state'] = 'finish'
			workflow['end_time'] = datetime.datetime.now()
		else:
			next_action = workflow['actions'][ok_to]
			if next_action['type'] == 'action':
				start_action(workflow_id,next_action)
				workflow['actions'][ok_to]['state'] = 'running'

			elif next_action['type'] == 'fork':
				for x in next_action['nextActions']:
					if workflow['actions'][x]['type'] == 'action':
						start_action(workflow_id,workflow['actions'][x])
						workflow['actions'][x]['state'] = 'running'
					elif workflow['actions'][x]['type'] == 'join':
						waitNum = workflow['actions'][x]['waitNum']-1
						workflow['actions'][x]['waitNum'] = waitNum
						if waitNum == 0:
							to = workflow['actions'][x]['to']
							if to == 'end':
								workflow['state'] = 'finish'
								workflow['end_time'] = datetime.datetime.now()
								db.worflows.save(workflow)
								return
							to_action=workflow['actions'][to]
							if to_action['type'] == 'action':
								start_action(workflow_id,to_action)
								workflow['actions'][to]['state']='running'
							else
								print('dag format error!') 
					else:
						print('dag format error!')

			elif next_action['type'] == 'join':
				waitNum = next_action['waitNum']-1
				workflow['actions'][ok_to]['waitNum'] = waitNum
				if waitNum == 0:
					to = next_action['to']
					if to == 'end':
						workflow['state'] = 'finish'
						workflow['end_time'] = datetime.datetime.now()
						db.worflows.save(workflow)
						return
					to_action=workflow['actions'][to]
					if to_action['type'] == 'action':
						start_action(workflow_id,to_action)
						workflow['actions'][to]['state']=running
					else
						print('dag format error!')


def listen_task(self):
	while True:
		print("waiting task...")
		task = self.rcon.brpop('task:queue', 0)[1]
		task=json.loads(task)
		execute_task(task)
		print "get task:",task
		# get_time = datetime.datetime.now()
		# task=json.loads(task)
		# submit_time = datetime.datetime.strptime(task['submit_time'], '%Y-%m-%d %H:%M:%S.%f')
		# wait_time = get_time - submit_time
		# wait_time = wait_time.total_seconds()  # 等待时间
		# self.execute(task)  # 执行任务
		# self.assemble_output(task['msg_id'],wait_time)  # 捕获输出
		time.sleep(0.1);
		print

if __name__ == '__main__':
	print 'listen task queue'
	Task().listen_task()

