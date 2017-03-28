from flask import Flask
from .views.home import home
from .views.problem import problem
from .views.training import training
from .views.visualization import visualization


app = Flask(__name__)
app.register_blueprint(home)
app.register_blueprint(problem)
app.register_blueprint(training)
app.register_blueprint(visualization)


