import base64
from urllib3 import PoolManager
import json
from constants import *
import pandas as  pd
import os
import threading
import sys
import jira.client

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 20)
pd.set_option('display.width', 5000)
pd.options.mode.chained_assignment = None

# JIRA_MODULE_NAME= 'Scorecard'
# JIRA_ISSUE_TYPE= 'Bug'


def jira_rest_call(data):
    # Set the root JIRA URL, and encode the username and password
    print('jira_url : {} , user_email_id : {}'.format(JIRA_URL,JIRA_USER_EMAIL_ID))
    authorization_param = base64.encodestring(('%s:%s' % (JIRA_USER_EMAIL_ID, JIRA_TOKEN)).encode()).decode().strip()

    manager = PoolManager(10)
    response = manager.request('POST', url=JIRA_URL, body=data,headers={'Content-Type': 'application/json', 'Authorization': 'Basic %s' % authorization_param})
    return json.loads(response.data.decode('utf-8'))


def create_product_task(ticket_id,summary,description,originalestimate) :
    # for ticket in ticket_list :
        # for module in module_list :
    json_data ={
        "fields":{
            "project":{
                "key":"{}".format(JIRA_PROJECT_ID)
            },
            "customfield_10900":[
                {
                    "value":"ALL"
                }
            ],
            "summary":"{}".format(summary),
            "description":"{}".format(description),
            "components":[
                {
                    "name":"Datamart",
                },
                {
                     "name" :"Scorecard"
                }
            ],
            "timetracking" : {
                    "originalEstimate": "{}".format(originalestimate)
                },

            "parent" : { "key" : "{}".format(ticket_id)},
            "issuetype":{
               "name":"Product-Task"
            }

        }
    }

    json_response = jira_rest_call(json.dumps(json_data))
    print('json_response : {} '.format(json_response))
    return json_response

def create_story_ticket(description, summary,sprint,originalEstimate,fixVersions):
    json_data = {
        "fields": {
            "project": {
                "key": "{}".format(JIRA_PROJECT_ID)
            },
            "customfield_11435":
                {
                    "value": "2020"
                }
            ,
            "customfield_10900": [
                {
                    "value": "ALL"
                }
            ],
            "customfield_10021": sprint
            ,

            "summary": str(summary),
            "components": [
                {
                    "name": "Datamart",
                },
                {
                    "name": "Scorecard"
                }
            ],

            "timetracking": {
                "originalEstimate": str(originalEstimate)
            },

            "fixVersions": [
                {
                    "name": str(fixVersions)
                }
            ],

            "issuetype": {
                "name": "Story"
            },
            "description": str(description)
        }
    }

    print(json_data)
    json_response = jira_rest_call(json.dumps(json_data))
    print('json_response : {} '.format(json_response))
    return json_response

def  get_issue_details(key,jira_details):
    global df
    issue=jira_details.issue(key)
    df1=pd.DataFrame([{"issue":issue,"summary":issue.fields.summary,"status":issue.fields.status,"tickettype":issue.fields.issuetype,"reporter":issue.fields.reporter,"assignee":issue.fields.assignee}])
    df=pd.concat([df,df1]).reset_index(drop=True)

if __name__ == '__main__':
    # OPTIONS : 1 ==> create product task for story ticket
    # OPTIONS : 2 ==> create story ticket
    # OPTIONS : 3 ==> get jira details
    option=sys.argv[1]
    try :
        ticket_list=sys.argv[2].split(',')
    except Exception as e:
        ticket_list=''
        print('no arg2 was passed !!')

    if option == '1' :
        json_keys = []
        df = pd.read_csv(str(os.getcwd()) + '/create_product_task.csv')
        # csv format  :  "ticket_id","description","summary","originalestimate"
        for index, row in df.iterrows():
            json_response = create_product_task(row['ticket_id'],row['summary'],['description'],row['originalestimate'])
            json_keys.append(json_response['key'])
        print('FOLLOWING PRODUCT TASKS WERE CREATED : ', json_keys)

    if option == '2' :
        json_keys = []
        df = pd.read_csv(str(os.getcwd()) + '/create_story_ticket.csv')
        # csv format  :  "description","summary","sprint","originalestimate","fixversions"

        for index, row in df.iterrows():
            json_response = create_story_ticket(row['description'], row['summary'], row['sprint'],
                                                row['originalestimate'], row['fixversions'])
            json_keys.append(json_response['key'])
        print('FOLLOWING TICKETS WERE CREATED : ', json_keys)

    elif option == '3' :
        jira_details = jira.JIRA('https://jirafigmd.atlassian.net', basic_auth=(JIRA_USER_EMAIL_ID, JIRA_TOKEN))

        threads = []
        df = pd.DataFrame()
        for x in ticket_list:
            if (len(x) > 0):
                t1 = threading.Thread(target=get_issue_details, args=(x, jira_details,))
                threads.append(t1)

        BATCH_SIZE = 10
        batch_threads = [threads[i:i + BATCH_SIZE] for i in range(0, len(threads), BATCH_SIZE)]
        for y in batch_threads:
            for x in y:
                x.start()
                # Wait for all of them to finish
            for x in y:
                x.join()
        print(df)
        print(len(df))
        df.to_csv('jira_details.csv')


