import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
	UPLOAD_FOLDER = os.getcwd()+'/data'

	@staticmethod
	def init_app(app):
		pass

class DevelopmentConfig(Config):
	MONGO_DBNAME = 'dagdb'
	MONGO_URI = 'mongodb://127.0.0.1:27017/dagdb'
	DOCKER_VERSION='1.35'

config = {
	'default': DevelopmentConfig
}