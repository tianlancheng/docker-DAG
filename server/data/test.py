import xml.etree.ElementTree as ET
import re,json,os
def get_namespace(element):
	m = re.match('\{.*\}', element.tag)
	return m.group(0) if m else ''
def parse_file(filepath):
	tree = ET.parse(filepath)
	root = tree.getroot()
	workflow_name=root.get('name')
	namespace = get_namespace(root)

	start_to=root.find(namespace+'start').get('to')
	end_name=root.find(namespace+'end').get('name')
	
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
		"content":node.find(zzjz_namespace+'zzjz').find(zzjz_namespace+'content').text,
		"ok_to":ok_to,
		"error_to":node.find(namespace+'error').get('to'),
		"isFinish":False
		}
		if(actions.has_key(ok_to)):
			item=actions.get(ok_to)
			if(item.get('type')=="join"):
				actions[ok_to]['waitNum']=actions[ok_to]['waitNum']+1
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
		"start": start_to,
		"end": end_name,
		"actions": actions
	}
	print(json.dumps(record))

	jsObj = json.dumps(record)  
	name = os.path.basename(filepath)
	path = os.path.dirname(filepath)
	shotname,extension= os.path.splitext(name);
	fileObject = open(shotname+'.json', 'w')  
	fileObject.write(jsObj)
	fileObject.close()
parse_file('hPDL.xml')


