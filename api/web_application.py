#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from flask import Flask, render_template, redirect, url_for, request
# from flask_mail import Mail, Message
import urllib
import json
import requests as rq
import os
import pandas as pd
import numpy as np
import warnings
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import requests

warnings.filterwarnings('ignore')


# In[ ]:


def establishConnection():
    credentials = service_account.Credentials.from_service_account_file(os.getcwd()+'/final-project-team2-3547e307824d.json')
    project_id = 'final-project-team2'
    client = bigquery.Client(credentials= credentials,project=project_id)
    return client

def getData():
    client = establishConnection()
    query = ("SELECT * FROM movielens.customers")
    query_job = client.query(
        query,# Location must match that of the dataset(s) referenced in the query.
        location="US",)  # API request - starts the query
    customer = query_job.to_dataframe()
#     customer.head()
    query = ("SELECT * FROM movielens.movies")
    query_job = client.query(
        query,# Location must match that of the dataset(s) referenced in the query.
        location="US",)  # API request - starts the query
    movies = query_job.to_dataframe()
    return customer,movies
#     movies.head()

def getCustomerInfo():
    firstnames = []
    lastnames = []
    emails = []
    mydictionary = dict()
    customer,movies = getData()
    for index, row in customer.iterrows():
        userId = row['user_id']
        firstnames.append({userId:row['first_name']})
        lastnames.append({userId:row['last_name']})
        emails.append({userId:row['email_id']})
        
    tmp = customer.set_index('user_id').T.to_dict('list')
    for key in sorted(tmp.keys()):
        mydictionary.update({key:tmp[key]})
        
    return firstnames,lastnames,emails, mydictionary

def getMovies():
    customer,movies = getData()
    moviesList = []
    for index,row in movies.iterrows():
        movieId = row['movie_id']
        moviesList.append({movieId:row['movie_title']})
    return moviesList

    
def recommendMovie(userId):
    data = {
        "Inputs":{
            "input1":
                {
                    "user_id": ["userId"],
                    "Values" : [[userId]]
                },
        },
        "GlobalParameters":{

        }
    }


    body = str.encode(json.dumps(data))
    url = 'https://ussouthcentral.services.azureml.net/workspaces/b5735f278ad44ea1b4f08ede6d289e5f/services/a1bf6e157b234b3bb5512a034b671763/execute?api-version=2.0&details=true'
    api_key = ' ' # Replace this with the API key for the web service
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
    
    req = urllib.request.Request(url, body, headers)
#     print(req)
    response = urllib.request.urlopen(req)
    result = response.read().decode('utf-8')
#     print (result)
    d = json.loads(result)
#     print(d)
#     print (d['Results']['output1']['value']['Values'])
    dlist = d['Results']['output1']['value']['Values']
    finalList = []
    for i in dlist:
        for j in range(len(i)):
            if j == 0:
                continue
            else:
                finalList.append(int(i[j]))
    return finalList

def insertData(userNum,recom):
    user = userNum
    recommendations = recom
    client = establishConnection()
    dataset_id = 'movielens'  # rep  lace with your dataset ID
    # For this sample, the table must already exist and have a defined schema
    table_id = 'recommended_movie'  # replace with your table ID
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # API request

    rows_to_insert = [(user,recommendations[0],recommendations[1],recommendations[2],recommendations[3],recommendations[4])]

    errors = client.insert_rows(table, rows_to_insert)  # API request

    assert errors == []


# ### Web Application Code

# In[ ]:


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/marketing', methods=['POST','GET'])
def marketing():
    if request.method == 'GET':
        method = 'GET'
        frstnames,lastnames,emails,mydict = getCustomerInfo()
#         userNum = request.form['userids']
        return render_template('marketing.html',methods=method, mydict=mydict)

@app.route('/result', methods=['POST','GET'])
def marketingResult():
    if request.method == 'POST':
        method = 'POST'
        frstnames,lastnames,emails,mydict = getCustomerInfo()
        userNum= int(request.form['userids'])
        return render_template('result.html',methods=method, userNum=userNum,frstnames=frstnames,lastnames=lastnames,
                               emails=emails,mydict=mydict)

@app.route('/recommendations', methods=['POST','GET'])
def marketingRecommendations():
    if request.method == 'POST':
        method = 'POST'
        frstnames,lastnames,emails,mydict = getCustomerInfo()
        userNum= int(request.form['userid'])
        firstname= request.form['firstname']
        lastname= request.form['lastname']
        fullname = firstname + " " + lastname
        email= request.form['email']
        recommendations = recommendMovie(userNum)
        moviesdata = getMovies()
        return render_template('recommendations.html',methods=method, userNum=userNum,firstname=firstname,lastname=lastname,
                               email=email,mydict=mydict,recommendations=recommendations, moviesdata=moviesdata)

@app.route('/sendingmails', methods=['POST','GET'])
def emailSend():
    if request.method == 'POST':
        email= request.form['email']
        userNum= int(request.form['userid'])
        recommendations = recommendMovie(userNum)
        moviesdata = getMovies()
        myList = []
        for r in recommendations:
            for ml in moviesdata:
                for key,values in ml.items():
                    if r == key:
                        myList.append(values)
        
        
        method = "POST"
        
        insertData(userNum,myList)
        
        requests.post("https://api.mailgun.net/v3/sandboxcf02a8cfc5854b26ab2faa632f779fa6.mailgun.org/messages",
              auth=("api", " API KEY"),
              data={
                      "from": "info7374@sandboxcf02a8cfc5854b26ab2faa632f779fa6.mailgun.org",
                        "to": [email],
                        "subject": "Here's your recent recommendation",
                        "html": render_template('template.html',myList=myList),
                        "t:text" : "yes"
              })
        return render_template('sendemail.html')
      
@app.route('/analytics')
def analytics():
    return render_template('analytics.html')

if __name__ == "__main__":
    app.run(debug=False)


# ### Code for inserting recommended movies 

# In[75]:


# cust,movies = getData()
# moviesdata = getMovies()

# myList = []
# for index,row in cust.iterrows():
#     user = row['user_id']
#     print(myList)
#     recommendations = recommendMovie(int(user))
#     for r in recommendations:
#         for ml in moviesdata:
#             for key,values in ml.items():
#                 if r == key:
#                     myList.append(values)
#     print(myList)                    
#     insertData(int(user),myList)
#     myList=[]

