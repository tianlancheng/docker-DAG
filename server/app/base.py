# -*- coding: utf-8 -*-

import functools
from flask import request
from flask import jsonify
import json

# get和表单参数
def require_args(*required_args):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            for arg in required_args:
                if not request.values.get(arg):
                    return jsonify(code=400, msg='error', data='parameter error'), 400
            return func(*args, **kw)
        return wrapper
    return decorator

# json对象字段
def require_json(*required_args):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            data = request.get_data()
            try:
                data = json.loads(data)
            except:
                return jsonify(code=400, msg='error', data='json parsing error'), 400
            for arg in required_args:
                if not data.get(arg):
                    return jsonify(code=400, msg='error', data='parameter error'), 400
            return func(*args, **kw)
        return wrapper
    return decorator