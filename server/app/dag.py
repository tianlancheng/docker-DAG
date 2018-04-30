from flask import Blueprint,request
from flask import jsonify
from base import require_args,require_json
import re,json,os
import xml.etree.ElementTree as ET
from werkzeug import secure_filename
import datetime
from app import app,mongo,rcon

dag = Blueprint('dag',__name__)


@dag.route('/index')
def index():
    return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflow',methods=['POST'])
def create_workflow():
	file = request.files['file']
	filename=secure_filename(file.filename)
	print 'filename= '+filename
	file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
	try:
		data=parse_file(app.config['UPLOAD_FOLDER']+'/'+filename)
	except:
		return jsonify(status=400, msg='parse error', data=None), 400
	
	return jsonify(status=200, msg='success', data=data), 200


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
	results=mongo.db.workflows.find(filters).sort("cteate_time",-1).skip(skip).limit(pageSize)
	data=[]
	for result in results:
		result['_id']=str(result['_id'])
		data.append(result)  
	return jsonify(status=200, msg='success', data=data), 200

@dag.route('/workflow/<id>',methods=['GET'])
def get_workflow(id):
	result=mongo.db.workflows.find_one({'_id':ObjectId(id)})
	result['_id']=str(result['_id'])
	return jsonify(status=200, msg='success', data=result), 200

@dag.route('/workflow/<id>/start',methods=['POST'])
def start_workflow(id):
	res=mongo.workflows.find_one({'_id':ObjectId(id)},{'state':1})
	if not res or res['state'] == 'running':
		return jsonify(status=400, msg='start error', data=None), 400
	task={
		"workflow_id":id,
		"type":"start"
	}
	rcon.lpush('action:queue', json.dumps(task))
	return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflow/<id>/stop',methods=['POST'])
def stop_workflow(id):
	res=mongo.workflows.find_one({'_id':ObjectId(id)},{'state':1})
	if not res or res['state'] != 'running':
		return jsonify(status=400, msg='stop error', data=None), 400
	mongo.db.workflows.update({"_id":ObjectId(id)},{"$set":{"isStop":True}})
	return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflow/task',methods=['POST'])
@require_json('workflow_id','type')
def add_task():
	task=request.get_data()
	rcon.lpush('task:queue', task)
	return jsonify(status=200, msg='success', data=None), 200

@dag.route('/workflow/<filename>',methods=['GET'])
def download_file(filename):
	print filename
	return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

@dag.route('/workflow/<id>/restart',methods=['POST'])
def restart_workflow(id):
	task={
		"workflow_id":id,
		"type":"restart"
	}
	rcon.lpush('task:queue', json.dumps(task))
	return jsonify(status=200, msg='success', data=None), 200

def save_to_file(filepath,record_dict):
	jsObj = json.dumps(record_dict)
	name = os.path.basename(filepath)
	path = os.path.dirname(filepath)
	shotname,extension= os.path.splitext(name);
	fileObject = open(path+'/'+shotname+'.json', 'w')  
	fileObject.write(jsObj)
	fileObject.close()

def get_namespace(element):
	m = re.match('\{.*\}', element.tag)
	return m.group(0) if m else ''
# {
# 	"workflow_name":"",
#	"isStop":False,
# 	"state":"",
# 	"start":"",
# 	"end":"",
# 	"start_time":"",
# 	"end_time":"",
# 	"actions":{
# 		"actionA":{
# 			"name":"actionA",
# 			"type":"action",
# 			"content":"",
# 			"execute_time":"",
# 			"ok_to":"",
# 			"error_to":"",
# 			"state":None
# 			},
# 		"forkA":{
# 			"name":"forkA",
# 			"type":"fork",
# 			"nextActions":[]
# 		},
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
	workflow_name=root.get('name')

	namespace = get_namespace(root)
	start = root.find(namespace+'start').get('to'),
	end = root.find(namespace+'end').get('name'),
	
	actions={}
	for node in root.findall(namespace+'join'):
		name=node.get('name')
		body={
			"name":name,
			"type":"join",
			"waitNum":0,
			"to":node.get('to')
		}
		actions[name]=body

	zzjz_namespace='{uri:oozie:zzjz-action:0.1}'
	for node in root.findall(namespace+'action'):
		name=node.get('name')
		ok_to=node.find(namespace+'ok').get('to')
		body={
		"name":name,
		"type":"action",
		"execute_time":None,
		"content":node.find(zzjz_namespace+'zzjz').find(zzjz_namespace+'content').text,
		"ok_to":ok_to,
		"error_to":node.find(namespace+'error').get('to'),
		"state":None
		}
		if(actions.has_key(ok_to)):
			item=actions.get(ok_to)
			if(item.get('type')=="join"):
				actions[ok_to]['waitNum']=actions[ok_to]['waitNum']+1
				actions[ok_to]['preAction'].append(name)
		actions[name]=body

	for node in root.findall(namespace+'fork'):
		name=node.get('name')
		nextActions=[]
		for subNode in node:
			nextActions.append(subNode.get('start')) 
		body={
			"name":name,
			"type":"fork",
			"nextActions":nextActions
		}
		actions[name]=body
	record={
		"workflow_name": workflow_name,
		"state": "create",
		"isStop":False,
		"create_time":datetime.datetime.now(),
		"start_time":None,
		"end_time":None,
		"start": start,
		"end": end,
		"actions": actions
	}
	# print(record)
	save_to_file(filepath,record)
	id=mongo.db.workflows.insert(record)
	record=algorithm.find_one({'_id':id})
	record['_id']=str(record['_id'])
	return record