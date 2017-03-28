from flask import Blueprint, render_template

problem = Blueprint('problem', __name__)


@problem.route('/problem', methods=['POST', 'GET'])
def problems():
    return render_template("problem.html")
