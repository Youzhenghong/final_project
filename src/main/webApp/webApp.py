from flask import Flask

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return return render_template("index.html")


if __name__ == '__main__':
    app.run(debug=True)