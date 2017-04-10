from flask import Blueprint, render_template,redirect,url_for, request,jsonify
import time
import random,os
from multiprocessing import Process, Pipe
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
        response['result'] = task.get()
    return jsonify(response)


def getOutput(rdp, f):
    info = rdp.readline()
    f.write(info)


@celery.task(bind=True)
def spark_job_task(self):
    r, w = os.pipe()
    wdp = os.fdopen(w, 'w')
    rdp = os.fdopen(r,'r')
    task_output = subprocess.Popen('spark-submit \
            --class "SimpleApp" \
            --master local[4] \
            /Users/youzhenghong/practice/scalasrc/target/scala-2.11/simple-project_2.11-1.0.jar', shell=True, stdout=subprocess.PIPE)
    f = open('/Users/youzhenghong/log.txt', 'w')
    while True:
        output = task_output.stdout.readline()
        if output == '' and task_output.poll() is not None:
            break
        if output:
            f.write(output)

    task_output.wait()
    f.close()
    print ("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    return {'result': task_output.communicate()[0]}



