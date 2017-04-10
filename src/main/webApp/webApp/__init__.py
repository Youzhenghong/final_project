from flask import Flask
app = Flask(__name__)

from celery import Celery
# Initialize Celery
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/1'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/1'
app.config["CELERY_ACCEPT_CONTENT "] = ['pickle', 'json', 'msgpack', 'yaml']
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


from webApp import redisdb
conn = redisdb.ConctDB()


from .views.home import home
from .views.problem import problem
from .views.training import training
from .views.visualization import visualization


app.register_blueprint(home)
app.register_blueprint(problem)
app.register_blueprint(training)
app.register_blueprint(visualization)
app.secret_key = 'jS\x96\x11<5FaS N\xb8Ci\x8a[\xf3\x8a%\xd4,\x89C'





