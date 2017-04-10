from flask import Blueprint, render_template, session, redirect, url_for,request
import pandas as pd
import config

problem = Blueprint('problem', __name__)


@problem.route('/problem', methods=['POST', 'GET'])
def problems():
    return render_template("problem.html")


@problem.route('/userlog/data',methods=['POST', 'GET'])
def getUserlogData():
    pageNum = request.args.get('page')
    if pageNum:
        page = int(pageNum)
    print(page)
    PATH = config.USER_LOG_PATH
    data_df = pd.read_csv(PATH, skiprows=range(1, (page-1)*10 + 1), nrows=10)
    print(data_df.to_json(orient="records"))
    return data_df.to_json(orient="records")




@problem.route('/userprofile/data',methods=['POST','GET'])
def getUserProfileData():
    pageNum = request.args.get('page')
    if pageNum:
        page = int(pageNum)
        print page
    elif 'profile_page' in session:
        page=session['profile_page']
    else:
        page=session['profile_page']=1
    PATH = config.USER_INFO_PATH
    data_df = pd.read_csv(PATH, skiprows=range(1, (page-1)*10 + 1), nrows=10)
    print(data_df.to_json(orient="records"))
    return data_df.to_json(orient="records")

@problem.route('/userprofile/nextpage')
def nextProfilePage():
    session['profile_page'] += 1
    redirect(url_for(getUserProfileData))

