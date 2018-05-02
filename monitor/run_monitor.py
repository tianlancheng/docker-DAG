import docker
from config import Config
import redis
import json
import time
import datetime

#connect to redis
pool = redis.ConnectionPool(host = Config.REDIS_HOST,port = Config.REDIS_PORT,db = 0)
rcon = redis.StrictRedis(connection_pool = pool)

dockerClient = docker.DockerClient(base_url = 'unix://var/run/docker.sock', version = Config.DOCKER_VERSION)


def cycle():
	while(True):
		services = dockerClient.services.list(filters = {"label":"task"})
		for service in services:
			tasks = service.tasks()
			isFinish = True
			error = None
			for task in tasks:
				if task['DesiredState'] == 'running':
					isFinish = False
				if task['Status']['State'] == 'failed':
					error = task['Status']['Err']

			if isFinish:
				labels = service.attrs['Spec']['Labels']
				createTime = service.attrs['CreatedAt'][0:26]+'Z'
				createTime = datetime.datetime.strptime(createTime, '%Y-%m-%dT%H:%M:%S.%fZ')
				nowTime = datetime.datetime.utcnow()
				executeTime = (nowTime-createTime).total_seconds()
				data={
					"type": "action",
					"workflowId": labels['workflowId'],
					"actionName": labels['actionName'],
					"error": error,
					"executeTime": executeTime
				}
				print(data)
				rcon.lpush('task:queue', json.dumps(data))
				service.remove()
		time.sleep(1)

if __name__ == '__main__':
	cycle()

