import psycopg2
import json
import boto3
import os
import requests

def handler(event,context):
    body = event['Records'][0]['Sns']['Message']
    body = json.loads(body)
    master_id = body['master_id']
    queue_id = body['queue_id']
    mrn = body['mrn']
    event_type = body['event']
    print(event_type)
    print(master_id)
    if event_type == 'new':
        post_new_patient(master_id)
        update_task_available(queue_id)
    elif event_type == 'update':
        post_demographics(master_id, mrn)
        update_task_available(queue_id)
    elif event_type == 'event':
        post_event(queue_id)
        update_task_available(queue_id)
    elif event_type == 'new_patient_and_event':
        post_patient_and_event(master_id, queue_id)
        update_task_available(queue_id)

ssm = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param = ssm.get_parameter(Name='uck-etl-db-prod-masterdata', WithDecryption=True )
db_request = json.loads(param['Parameter']['Value']) 

def masterdata_conn():
    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    conn.autocommit = False
    return conn

url = os.environ['URL']
headers= {'Content-Type': 'application/x-www-form-urlencoded'}

def post_new_patient(master_id):
    with masterdata_conn() as _targetconnection:
        with _targetconnection.cursor() as cur:
            get_new_patient_call = f"call public.mahler_get_new_patient('{master_id}', '{{}}')"
            try:
                cur.execute(get_new_patient_call)
                _targetconnection.commit()
            except Exception as e:
                print(f"Error executing upsert_mrn_call: {str(e)}")         
            mahler_post_payload = cur.fetchone()
            mahler_post_payload = json.dumps(mahler_post_payload)
            data = json.loads(mahler_post_payload)[0]
            print(data)
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
            _targetconnection.close()
            post_demographics(master_id, mrn)

def post_demographics(master_id, mrn):
    with masterdata_conn() as _targetconnection:
        with _targetconnection.cursor() as cur:
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
            _targetconnection.close()

def post_event(queue_id):
    with masterdata_conn() as _targetconnection:
        with _targetconnection.cursor() as cur:
            get_event_call = f"call public.mahler_get_event('{queue_id}','{{}}');"
            try:
                cur.execute(get_event_call)
                _targetconnection.commit()
            except Exception as e:
                print(f"Error executing get_event_call: {str(e)}")
            mahler_event_payload = json.dumps(cur.fetchone())
            print(mahler_event_payload)
            event_data = json.loads(mahler_event_payload)[0]   
            pond_event_id = event_data['schedule_notes']
            pond_event_id = pond_event_id.rsplit("_", 1)[-1]
            event_data['username'] = os.environ['USERNAME1']
            event_data['key'] = os.environ['MAHLER_KEY']
            event_data['action'] = 'api_schedule_event'     
            event_response = requests.post(url, headers=headers, data=event_data)
            print(event_response.text)
            mahler_event_id = json.loads(event_response.text)
            mahler_event_id = mahler_event_id['event_id']
            insert_call = f"insert into mahler_event_cx(pond_visit_id,mahler_event_id) values({pond_event_id}, {mahler_event_id});"
            try:
                cur.execute(insert_call)
                _targetconnection.commit()
            except Exception as e:
                print(f"Error executing insert_event: {str(e)}")
            _targetconnection.close()

def post_patient_and_event(master_id, queue_id):
    post_new_patient(master_id)
    post_event(queue_id)

def update_task_available(queue_id):
    with masterdata_conn() as _targetconnection:
        with _targetconnection.cursor() as cur:
            update_task_call = f"call public.mahler_update_task_available('{queue_id}');"
            try:
                cur.execute(update_task_call)
                _targetconnection.commit()
            except Exception as e:
                print(f"Error executing update_task_call: {str(e)}")
            _targetconnection.close()
