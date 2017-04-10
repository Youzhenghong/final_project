from flask import Blueprint, render_template,redirect,url_for, request,jsonify
import time
import random,os
from webApp import celery
from webApp import conn
import subprocess
training = Blueprint('training', __name__)


@training.route('/training', methods=['POST','GET'])
def trainingPage():

    return render_template('training.html')


@training.route("/training/command", methods=["POST","GET"])
def parseCommand():
    cmd = request.args.get('command')
    print cmd
    service_url = parse(cmd)
    return jsonify({'service_url': service_url})

def parse(cmd):
    print cmd
    return url_for(".sparktask")


@training.route('/sparktask', methods=['POST'])
def sparktask():
    task = spark_job_task.apply_async()
    return jsonify({}), 202, {'Location': url_for('.taskstatus', task_id=task.id)}


@training.route('/status/<path:task_id>')
def taskstatus(task_id = 1, methods=['GET']):
    task = spark_job_task.AsyncResult(task_id)
    response = {
        'state': task.state,
    }
    if(task.state == "SUCCESS"):
        response['result'] = task.result
    return jsonify(response)


def getOutput(r, task_output):
    rdp = os.fdopen(r)
    while task_output.poll() is None:
        info = rdp.readline()
        print info


@celery.task(bind=True)
def spark_job_task(self):
    r, w = os.pipe()
    rdp = os.fdopen(r, 'r')
    wdp = os.fdopen(w, 'w')
    task_output = subprocess.Popen('spark-submit \
            --class "SimpleApp" \
            --master local[4] \
            /Users/youzhenghong/practice/scalasrc/target/scala-2.11/simple-project_2.11-1.0.jar', shell=True, stdout=wdp)
    pid = os.fork()
    if pid == 0:
        getOutput(rdp, task_output)


    print ("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    return {'result': "completed"}





# some helpful sense code:
# http://sense.qbox.io/gist/4f7ed0e6aa51f6badd9d27de979741e4b8768205

