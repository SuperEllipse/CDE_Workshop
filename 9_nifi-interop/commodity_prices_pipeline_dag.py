from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator   
import time
# from utils import (
#     get_http_response, 
#     make_put_request, 
#     get_nifi_processor_clientid_and_version, 
#     get_primary_nodeId_and_address,
#     fetch_state_value
# )
from requests.auth import HTTPBasicAuth
import requests
import json
from base64 import b64encode 
import time
from datetime import datetime, timedelta

user_id = Variable.get('user_id')
password = Variable.get('password')


def get_http_response(url, user_id, password):
    basic = HTTPBasicAuth(user_id, password)
    response = requests.get(url, auth=basic, verify=False)
    print("returned :" , response.text)
    response.raise_for_status()  # Raise an exception for error responses

    return response.json() 

def make_put_request(url, data, username, password, content_type="application/json"):
    """
    Makes a PUT request to the specified URL with basic authentication.

    Args:
        url (str): The URL of the resource to update.
        data (dict or str): The data to send in the request body.
            If a dictionary, it will be converted to JSON.
        username (str): The username for basic authentication.
        password (str): The password for basic authentication.
        content_type (str, optional): The content type of the request body.
            Defaults to "application/json".

    Returns:
        requests.Response: The response object containing the server's response.
    """

    if isinstance(data, dict):
        data = json.dumps(data)  # Convert dictionary to JSON if necessary

    # Combine username and password for basic authentication
    auth_string = b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth_string}", "Content-Type": content_type}

    try:
        response = requests.put(url, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error making PUT request: {e}")
        return None

def get_nifi_processor_clientid_and_version(url, user_id, password):
    data =get_http_response(url, user_id, password) 

    print(data["revision"]) # {'clientId': 'c74a3773-cee5-15db-6b8c-7c5873a52dd7', 'version': 16}
    return data["revision"]["clientId"], data["revision"]["version"]

def get_primary_nodeId_and_address(url, user_id, password) :
    data =get_http_response(url, user_id, password) 
    primary_node = None

    for node in data["cluster"]["nodes"]:
        print(node)
        if "Primary Node" in node["roles"] : 
            print("Primary:", node)
            primary_node = node
            break
        
    if primary_node:
    # When Deploying to Airflow we need to uncomment this 
    #     kwargs['ti'].xcom_push(key='primary_node_id', value=primary_node['nodeId'])
    #     kwargs['ti'].xcom_push(key='primary_node_address', value=primary_node['address'])
        # COMMENT these 3 lines for Airflow and uncomment the earlier lines for Airflow
        print( "Nodeid : ", primary_node['nodeId'])
        print( "Nodeaddress : ", primary_node['address'])    
        return primary_node['nodeId'], primary_node['address']
    else:
        raise Exception("Primary node not found in API response.")
    
def fetch_state_value(url, state_property, primary_nodeId,user_id, password) :
# let us call this first time for the flow 
    data =get_http_response(url, user_id, password)     
    print(data["componentState"]["localState"]["state"])

    processor_states = data["componentState"]["localState"]["state"]
    for state in processor_states:
        if state["clusterNodeId"] == primary_nodeId and state["key"] == state_property :
            date_str=state["value"]
            print(date_str)
            converted_date=datetime.strptime(date_str, "%a %b %d %H:%M:%S UTC %Y")
            print(converted_date)
    return converted_date



# start_processor_id = "39793bc3-7869-1e73-8b15-8c2b6c6ebf25" # for testing use "0d082996-0192-1000-ffff-fffff4de0cfe"
# end_processor_id = "c74a337b-cee5-15db-ba2f-0fc29c1aa05a" # for testing use 0d11f872-0192-1000-ffff-ffff8cdff6f2

start_processor_id = "effa3ae8-49b5-1782-8275-e030ee5088dc" 
end_processor_id = "c74a337b-cee5-15db-ba2f-0fc29c1aa05a" 

#define All these values that need to be moved to Airflow Variables later
cluster_url = "https://se-indonesia-cfm1-management0.se-indon.a465-9q4k.cloudera.site/se-indonesia-cfm1/cdp-proxy-api/nifi-app/nifi-api/controller/cluster"  # Replace with your actual API endpoint
url_nifi_start_processor=f"https://se-indonesia-cfm1-management0.se-indon.a465-9q4k.cloudera.site/se-indonesia-cfm1/cdp-proxy-api/nifi-app/nifi-api/processors/{start_processor_id}"
run_url = f"https://se-indonesia-cfm1-management0.se-indon.a465-9q4k.cloudera.site/se-indonesia-cfm1/cdp-proxy-api/nifi-app/nifi-api/processors/{start_processor_id}/run-status"
end_nifiprocessor_url = f"https://se-indonesia-cfm1-management0.se-indon.a465-9q4k.cloudera.site/se-indonesia-cfm1/cdp-proxy-api/nifi-app/nifi-api/processors/{end_processor_id}/state"
state_property="last_tms"


def prepare(**kwargs):
    """Where something happens before the NiFi pipeline is triggered."""
    primary_nodeId , _ = get_primary_nodeId_and_address(cluster_url, user_id, password)
    kwargs['ti'].xcom_push(key='primary_nodeId', value=primary_nodeId)


def startup(**kwargs):
    # Initialize the setup
    primary_nodeId = kwargs['ti'].xcom_pull(key='primary_nodeId')
    print("Start Up : Primary Node ID ", primary_nodeId)

    # Let us get the Client Id and Version Id
    clientId, version = get_nifi_processor_clientid_and_version(url_nifi_start_processor, user_id, password)
    print(f"ClientId: {clientId}  \n version: {version}")


    # Let us get the output of the end processor    # Added here since we need to get the endtime different from start time
    state_property="last_tms"
    initial_val = fetch_state_value(end_nifiprocessor_url, state_property, primary_nodeId, user_id, password)
    print("Initial End Processor value : ", initial_val)  
    kwargs['ti'].xcom_push(key='initial_val', value=initial_val)


    #prepare payload data  and set mode to running
    mode = "RUNNING" 
    payload_data = {"revision": {"clientId": clientId, "version": version}, "state": mode, "disconnectedNodeAcknowledged": True}
    response = make_put_request(run_url, payload_data, user_id, password)

    if response:
        print(f"RUNNING - Request successful! Response status code: {response.status_code}")
        print(response.text)  # Access the response content
    else:
        print("RUNNING - Request failed.")    

    #let us wait for seconds 
    time.sleep(2)

    # Now Let us Stop the Processor to prevent overflow
    clientId, version = get_nifi_processor_clientid_and_version(url_nifi_start_processor, user_id, password)
    mode = "STOPPED"
    payload_data = {"revision": {"clientId": clientId, "version": version}, "state": mode, "disconnectedNodeAcknowledged": True}

    response = make_put_request(run_url, payload_data, user_id, password)

    if response:
        print(f"Request successful! Response status code: {response.status_code}")
        print(response.text)  # Access the response content
    else:
        print("Request failed.")
                     

def wait_for_update(**kwargs):
    primary_nodeId = kwargs['ti'].xcom_pull(key='primary_nodeId')
    print("Start Up : Primary Node ID ", primary_nodeId)
    
    initial_val = kwargs['ti'].xcom_pull(key='initial_val')
    # # Let us get the output of the end processor    
    # state_property="last_tms"
    # initial_val = fetch_state_value(end_nifiprocessor_url, state_property, primary_nodeId, user_id, password)
    # print("Initial End Processor value : ", initial_val)     
    

    # query and wait until an update happens or we time out.
    while True:
        current_val = fetch_state_value(end_nifiprocessor_url, state_property, primary_nodeId, user_id, password)
        if initial_val == current_val :
            print("Waiting...")
            time.sleep(5)
        else:
            print(f"Update found: {current_val}")
            break 



def finalize():
    # add code for the CDE Job to be called here
    pass


default_args = {    
    'owner': 'vishrajagopalan',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0    
}
with DAG(
    'commodity_prices_pipeline_dag',
    default_args=default_args,
    schedule_interval='0 * * * *',  # Set this to run hourly 
    catchup=False,
    is_paused_upon_creation=False
) as dag:   

    preparation = PythonOperator(
        task_id="preparation",
        python_callable=prepare,
    )
    startup_task = PythonOperator(
        task_id="startup_task",
        python_callable=startup,
    )

    waiting_task = PythonOperator(
        task_id="waiting_task",
        python_callable=wait_for_update,
    )

    finalization = PythonOperator(
        task_id="finalization",
        python_callable=finalize,
    )
    cde_commodity_prices_job  = CDEJobRunOperator(
        job_name='Commodity_all_aggregations',
        task_id='cde_job_Commodity_all_aggregations',
    )
    preparation >> startup_task >> waiting_task >> finalization >> cde_commodity_prices_job
