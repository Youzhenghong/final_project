from flask import Blueprint, render_template,redirect,url_for, request,jsonify, Response
from config import SPARK_MESSAGE_QUEUE as spark_messege_queue
from webApp import celery, conn
import subprocess
import os, time,json, random, pickle, signal
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
    return jsonify({}), 202, {'Location': url_for('.taskstatus', task_id=task.id),'task_id': task.id}


@training.route('/status/<path:task_id>')
def taskstatus(task_id = 1, methods=['GET']):
    task = spark_job_task.AsyncResult(task_id)
    response = {
        'task_state': task.state,
        'info': ""
    }
    while not conn.isEmpty(spark_messege_queue):
        info = conn.pop(spark_messege_queue)
        response['info'] += info + "\n"

    #print response['info']
    return jsonify(response)


def getOutput(rdp, f):
    info = rdp.readline()
    f.write(info)


@celery.task(bind=True)
def spark_job_task(self):
    time.sleep(3)
    task_output = subprocess.Popen('spark-submit \
            --class "SimpleApp" \
            --master local[4] \
            /Users/youzhenghong/practice/scalasrc/target/scala-2.11/simple-project_2.11-1.0.jar', shell=True, stdout=subprocess.PIPE)
    pid = os.fork()
    if pid == 0:
        publish()
    while True:
        output = task_output.stdout.readline()
        if output == '' and task_output.poll() is not None:
            break
        if output:
            conn.push(spark_messege_queue, output)

    task_output.wait()
    os.kill(pid, signal.SIGKILL)
    print ("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    return {'result': task_output.communicate()[0]}


@training.route('/map/chinageojson', methods=['GET','POST'])
def getChinaMap():
    path = "/Users/youzhenghong/final_project/src/main/webApp/webApp/static/china.geojson"
    print path
    with open(path, 'r') as f:
        geoJson = json.load(f)
        print type(geoJson)
        return jsonify(geoJson)


# event source router
@training.route("/training/result", methods=['GET','POST'])
def getTrainingResult():
    res = Response(event_stream(), mimetype="text/event-stream")
    print res
    return res


# test function for publish
def publish():
    while True:
        data = {
            'user_id': random.randint(0, 1000),
            'merchant_id': random.randint(0, 100),
            'user_loc': [random.uniform(80, 120),random.uniform(10, 40)],
            'merchant_loc': [random.uniform(80, 120), random.uniform(10, 40)],
            'prob': random.random()
        }
        print data
        conn.publish('result', json.dumps(data))
        #time.sleep(1)


# receive redis pub stream
def event_stream():
    pubsub = conn.pubsub()
    pubsub.subscribe('result')
    for item in pubsub.listen():
        if item['type'] == 'message':
            data = pickle.loads(item['data'])
            yield 'data: %s\n\n' % data



