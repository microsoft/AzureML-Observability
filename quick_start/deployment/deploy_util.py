import random
import subprocess
from aml_obs import KV_SP_ID, KV_SP_KEY
import urllib.request
import json
import pandas as pd


def deploy_model(ws, online_endpoint_name=None,create_endpoint=True, install_cli_v2=True, deploy_model=True):

    kv = ws.get_default_keyvault()
    ws_detail = ws.get_details()
    ws_name = ws_detail['name']
    tenant_id = ws_detail['identity']['tenant_id']
    location = ws_detail['location']
    subscription_id = ws_detail['id'].split("/")[2]
    resource_group_name = ws_detail['id'].split("/")[4]

    if install_cli_v2:
        library_install_cmd = f"""
        az extension add -n ml -y --version 2.3.1
        az configure --defaults group={resource_group_name} workspace={ws_name} location={location}   
        """
        print("Install ML CLI v2")
        
        subprocess.check_output(library_install_cmd, shell=True)

    
    if online_endpoint_name is None:
        print("Creating managed online endpoint  ")

        online_endpoint_name = "iris-ep" + str(random.randint(0,9999))
        
        while True:
            try:
                print("creating online endpoint with name ", online_endpoint_name)
                online_ep_cmd = f"az ml online-endpoint create -f deployment/endpoint.yml --name {online_endpoint_name}"
                print(online_ep_cmd)
                subprocess.check_output(online_ep_cmd,shell=True)
                break
            except:
                print("Online EP creation failed, probably name is not unique, try again with new name")
                online_endpoint_name = "iris-ep" + str(random.randint(0,9999))

    
    else:
        if create_endpoint:
            online_ep_cmd = f"az ml online-endpoint create -f deployment/endpoint.yml --name {online_endpoint_name}"
            print("creating online endpoint with supplied name ",online_endpoint_name )
            subprocess.check_output(online_ep_cmd,shell=True)
    if deploy_model:
        print("Deploy ML model ")
        sp_id = kv.get_secret(KV_SP_ID)
        sp_secret = kv.get_secret(KV_SP_KEY)
        deployment_cmd = f"az ml online-deployment create --name blue --endpoint {online_endpoint_name}  -f deployment/realtime_score.yml --set environment_variables.TENANT_ID={tenant_id} environment_variables.TABLE_NAME=IRIS_MODEL  environment_variables.SUBSCRIPTION_ID={subscription_id} environment_variables.RG={resource_group_name} environment_variables.WS_NAME={ws_name} environment_variables.SP_ID={sp_id} environment_variables.SP_SECRET={sp_secret} --all-traffic"
        subprocess.check_output(deployment_cmd,shell=True)
        print("deployment is done")

    scoring_key =subprocess.check_output(f"az ml online-endpoint get-credentials -n {online_endpoint_name} --query primaryKey", shell=True).decode("utf-8").strip().strip('\"')
    scoring_uri =subprocess.check_output(f"az ml online-endpoint show -n {online_endpoint_name} --query scoring_uri", shell=True).decode("utf-8").strip().strip('\"')
    return scoring_uri, scoring_key



def score_test(scoring_uri, scoring_key):
    dataset = pd.read_csv("https://azuremlexamples.blob.core.windows.net/datasets/iris.csv")
    X= dataset[['sepal_length','sepal_width','petal_length','petal_width']]
    data = X.sample(10)
    body = str.encode(json.dumps({"data":data.to_dict()}))

    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ scoring_key)}

    req = urllib.request.Request(scoring_uri, body, headers)

    try:
        response = urllib.request.urlopen(req)

        result = response.read()
        print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))

