# import pandas as pd
import psycopg2
import json
# import pandas.io.sql as sqlio
# pd.set_option('display.max_columns', 500)
import boto3
import os
# from loguru import logger
import requests
# from jinja2.loaders import FileSystemLoader
# from jinja2.environment import Environment
# from jinja2.loaders import FileSystemLoader
# from jinja2.environment import Environment
import sys


# def handler(event,context):

#     payload = event
#     body = event['Records'][0]['Sns']['Message']
#     logger.info('got here', event)
#     body = json.loads(body)
#     print(body)
#     insval_process(body)

ssm = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param = ssm.get_parameter(Name='uck-etl-db-prod-masterdata', WithDecryption=True )
db_request = json.loads(param['Parameter']['Value']) 
# env = Environment(extensions=['jinja2.ext.loopcontrols'])
# env.loader = FileSystemLoader(os.path.dirname(__file__) + '/templates')

def masterdata_conn():
    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn


def post_to_mahler(queue_id, master_id, mrn, event_type):
    _targetconnection = masterdata_conn()
    cur = _targetconnection.cursor()
    # get_mrn = f"select mahler_client_id from mahler_id_cx mic where mic.master_id = {master_id};"
    # cur.execute(get_mrn,)
    # mrn = cur.fetchone()
    # if mrn = 'new patient' then post to mahler, get mahler id, call upsert proc, get demographics, send to mahler
    # if mrn = mrn then send to mahler
    url = os.environ['URL']
    headers= {'Content-Type': 'application/x-www-form-urlencoded'}

    if event_type == 'new':
         get_new_patient_call = f"call public.mahler_get_new_patient('{master_id}', '{{}}')"
         try:
            cur.execute(get_new_patient_call)
            _targetconnection.commit()
         except Exception as e:
            print(f"Error executing upsert_mrn_call: {str(e)}")         
         mahler_post_payload = cur.fetchone()
         mahler_post_payload = json.dumps(mahler_post_payload)
         # mahler_post_payload = json.loads(mahler_post_payload)[0]
         # get_mrn_template = env.get_template('get_mrn.json')
         # data = json.loads(get_mrn_template.render(mahler_post_payload))
         data = json.loads(mahler_post_payload)[0]
         # add username and key from env variables
         data['username'] = os.environ['USERNAME1']
         data['key'] = os.environ['MAHLER_KEY']
         data['action'] = 'api_initial_client_register'
         print(json.dumps(data))
         response = requests.post(url, headers=headers, data=data)
         print(response.text)
         response = json.loads(response.text)
         mahler_client_id = response['client_id']
         print(mahler_client_id) 
         upsert_mrn_call = f"call public.mahler_upsert_mrn('{master_id}', {mahler_client_id});"
         print(upsert_mrn_call)
         try:
            cur.execute(upsert_mrn_call)
            _targetconnection.commit()
         except Exception as e:
            print(f"Error executing upsert_mrn_call: {str(e)}")
         mrn = mahler_client_id
    else:
        #placeholder assnmnt
        print(mrn)

    get_demographics_call = f"call public.mahler_get_demographics('{master_id}', {mrn}, '{{}}');"
    try:
        cur.execute(get_demographics_call)
        _targetconnection.commit()
    except Exception as e:
        print(f"Error executing upsert_mrn_call: {str(e)}")
    mahler_full_demo_payload = json.dumps(cur.fetchone())
    
    print(mahler_full_demo_payload)
    
    demo_data = json.loads(mahler_full_demo_payload)[0]   
    demo_data['username'] = os.environ['USERNAME1']
    demo_data['key'] = os.environ['MAHLER_KEY']
    demo_data['action'] = 'api_update_client_information'     
    demo_response = requests.post(url, headers=headers, data=demo_data)
    print(demo_response.text)





post_to_mahler(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
#test- get the ids from sysargv

