from flask import Blueprint, render_template

problem = Blueprint('problem', __name__)


@problem.route('/problems', methods=['POST', 'GET'])
def problems():
    return "adsfasdf"
