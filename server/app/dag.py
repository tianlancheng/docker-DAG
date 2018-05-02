# -*- coding: utf-8 -*-
from flask import Blueprint,request
from flask import jsonify
from base import require_args,require_json
import re,json,os
import xml.etree.ElementTree as ET
from werkzeug import secure_filename
import datetime
from app import app,mongo,rcon
from bson.objectid import ObjectId
import tools
import shutil

dag = Blueprint('dag',__name__)

@dag.route('/template',methods=['POST'])
def add_template():
	file = request.files['file']
	filename=secure_filename(file.filename)
	print 'filename= '+filename
	file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
	try:
		data=parse_file(app.config['UPLOAD_FOLDER']+'/'+filename)
	except Exception,e:
		print(e)
		return jsonify(status=400, msg='parse error', data=None), 400
	id=mongo.db.templates.insert(data)
	res=mongo.db.templates.find_one({'_id':id})
	res['_id']=str(res['_id'])
	return jsonify(status=200, msg='success', data=res), 200

@dag.route('/templates',methods=['GET'])
@require_args('currentPage','pageSize')
def get_templates():
	currentPage=int(request.args.get("currentPage"))
	pageSize=int(request.args.get("pageSize"))
	skip=(currentPage-1)*pageSize
	filters=request.args.get('filters')
	if not filters:
		filters={}
	else:
		filters=json.loads(filters)
	results=mongo.db.templates.find(filters).sort("cteateTime",-1).skip(skip).limit(pageSize)
	data=[]
	for result in results:
		result['_id']=str(result['_id'])
		data.append(result)  
	return jsonify(status=200, msg='success', data=data), 200

@dag.route('/template/<id>',methods=['PUT'])
def update_template(id):
	data = json.loads(request.get_data())
	res=mongo.db.templates.find_one({'_id':ObjectId(id)},{'_id':1})
	if not res:
		return jsonify(status=400, msg='can not find template', data=None), 400
	mongo.db.templates.update({"_id":ObjectId(id)},{"$set":data})
	res=mongo.db.templates.find_one({'_id':ObjectId(id)})
	res['_id']=str(res['_id'])
	return jsonify(status=200, msg='success', data=res), 200

@dag.route('/template/<id>',methods=['DELETE'])
def delete_template(id):
	res=mongo.db.templates.find_one({'_id':ObjectId(id)},{'_id':1})
	if not res:
		return jsonify(status=400, msg='can not find template', data=None), 400
	mongo.db.templates.remove({'_id':ObjectId(id)})
	return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflow/<workflowName>/start',methods=['POST'])
def start_workflow(workflowName):
	print("start a workflow:"+workflowName)
	try:
		file = request.files['file']
		filename=secure_filename(file.filename)
		print 'filename= '+filename
	except Exception,e:
		print(e)
		return jsonify(status=400, msg='can not upload file', data=None), 400

	template=mongo.db.templates.find_one({'workflowName':workflowName},{'_id':0})
	if not template:
		return jsonify(status=400, msg='can not find template', data=None), 400
	
	id=mongo.db.workflows.insert(template)
	res=mongo.db.workflows.find_one({'_id':id})
	res['_id']=str(res['_id'])

	workflowId=res['_id']
	savepath='/nfs-data/'+workflowId+'/input'
	tools.mkdir(savepath)
	file.save(os.path.join(savepath, filename))

	task={
		"workflowId": workflowId,
		"type":"start"
	}
	rcon.lpush('task:queue', json.dumps(task))

	return jsonify(status=200, msg='success', data=res), 200

@dag.route('/workflow/<id>/restart',methods=['POST'])
def restart_workflow(id):
	res=mongo.db.workflows.find_one({'_id':ObjectId(id)},{'state':1,'workflowName':1})
	if not res or res['state'] == 'running':
		return jsonify(status=400, msg='can not restart', data=None), 400
	template=mongo.db.templates.find_one({'workflowName':res['workflowName']},{'_id':0})
	if not template:
		return jsonify(status=400, msg='can not find template', data=None), 400
	mongo.db.workflows.update({"_id":ObjectId(id)},{"$set":template})
	task={
		"workflowId": id,
		"type": "start"
	}
	rcon.lpush('task:queue', json.dumps(task))
	return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflows',methods=['GET'])
@require_args('currentPage','pageSize')
def get_workflows():
	currentPage=int(request.args.get("currentPage"))
	pageSize=int(request.args.get("pageSize"))
	skip=(currentPage-1)*pageSize
	filters=request.args.get('filters')
	if not filters:
		filters={}
	else:
		filters=json.loads(filters)
	results=mongo.db.workflows.find(filters).sort("cteateTime",-1).skip(skip).limit(pageSize)
	data=[]
	for result in results:
		result['_id']=str(result['_id'])
		data.append(result)  
	return jsonify(status=200, msg='success', data=data), 200

@dag.route('/workflow/<id>',methods=['GET'])
def get_workflow(id):
	res=mongo.db.workflows.find_one({'_id':ObjectId(id)})
	if not res:
		return jsonify(status=400, msg='can not find workflow', data=None), 400
	res['_id']=str(res['_id'])
	return jsonify(status=200, msg='success', data=res), 200


@dag.route('/workflow/<id>/stop',methods=['POST'])
def stop_workflow(id):
	res=mongo.db.workflows.find_one({'_id':ObjectId(id)},{'state':1})
	if not res or res['state'] != 'running':
		return jsonify(status=400, msg='can not stop', data=None), 400
	mongo.db.workflows.update({"_id":ObjectId(id)},{"$set":{"isStop":True}})
	return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflow/<id>',methods=['DELETE'])
def delete_workflow(id):
	path='/nfs-data/'+id
	if os.path.exists(path):
		shutil.rmtree(path)
	res=mongo.db.workflows.find_one({'_id':ObjectId(id)},{'_id':1})
	if res:
		mongo.db.workflows.remove({'_id':ObjectId(id)})	
	return jsonify(status=200, msg='success', data=None), 200


@dag.route('/workflow/<filename>',methods=['GET'])
def download_file(filename):
	print filename
	return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)


def save_to_file(workflowName,recordDict):
	jsObj = json.dumps(recordDict)
	# name = os.path.basename(filepath)
	# path = os.path.dirname(filepath)
	# shotname,extension= os.path.splitext(name);
	fileObject = open(app.config['UPLOAD_FOLDER']+'/'+workflowName+'.json', 'w')  
	fileObject.write(jsObj)
	fileObject.close()

def get_namespace(element):
	m = re.match('\{.*\}', element.tag)
	return m.group(0) if m else ''
# {
# 	"workflowName":"",
#	"isStop":False,
# 	"state":"",
# 	"start":"",
# 	"end":"",
# 	"startTime":"",
# 	"endTime":"",
# 	"actions":{
# 		"actionA":{
# 			"name":"actionA",
# 			"type":"action",
#			"nodeid":"",
# 			"content":"",
# 			"executeTime":"",
#           "nextActions":[],
# 			"errorTo":"",
# 			"state":None
# 			},
# 		"joinD":{
# 			"name":"joinD",
# 			"type":"join",
#			"preActions":[],
# 			"waitNum":0,
# 			"to":""
# 		}
# 	},
# }
def parse_file(filepath):
	tree = ET.parse(filepath)
	root = tree.getroot()
	workflowName=root.get('name')

	namespace = get_namespace(root)
	start = root.find(namespace+'start').get('to')
	end = root.find(namespace+'end').get('name')
	
	actions={}
	joins={}
	forks={}
	for node in root.findall(namespace+'join'):
		name=node.get('name')
		body={
			"name":name,
			"type":"join",
			"waitNum":0,
			"preActions":[],
			"to":node.get('to')
		}
		joins[name]=body

	
	for node in root.findall(namespace+'fork'):
		name=node.get('name')
		nextActions=[]
		for subNode in node:
			actionName=subNode.get('start')
			nextActions.append(actionName) 
		forks[name]=nextActions

	zzjz_namespace='{uri:oozie:zzjz-action:0.1}'
	for node in root.findall(namespace+'action'):
		name=node.get('name')
		okTo=node.find(namespace+'ok').get('to')
		nextActions=[]
		if forks.has_key(okTo):
			nextActions=forks[okTo]
		else:
			nextActions.append(okTo)
		body={
		"name":name,
		"type":"action",
		"executeTime":None,
		"nodeid":node.find(zzjz_namespace+'zzjz').find(zzjz_namespace+'nodeid').text,
		"content":node.find(zzjz_namespace+'zzjz').find(zzjz_namespace+'content').text,
		"nextActions":nextActions,
		"errorTo":node.find(namespace+'error').get('to'),
		"state":None
		}
		for x in nextActions:
			if(joins.has_key(x)):
				joins[x]['waitNum']=joins[x]['waitNum']+1
				joins[x]['preActions'].append(name)
		actions[name]=body

	record={
		"workflowName": workflowName,
		"state": "create",
		"isStop":False,
		"startTime":None,
		"endTime":None,
		"start": start,
		"end": end,
		"actions": dict(actions.items()+joins.items())
	}
	# print(record)
	#save_to_file(workflowName,record)
	return record