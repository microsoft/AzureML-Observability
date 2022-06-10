from obs import KV_SP_ID, KV_SP_KEY, KV_ADX_DB, KV_ADX_URI, KV_TENANT_ID,SP_NAME_PF
from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import Cluster, AzureSku
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import Cluster, AzureSku,LanguageExtension, LanguageExtensionsList
from azure.mgmt.kusto.models import ReadWriteDatabase
from azure.mgmt.kusto.models import DatabasePrincipalAssignment
from azure.identity import ClientSecretCredential 

import time
import json
import subprocess
from datetime import timedelta


import random
def create_adx_cluster(resource_group_name, cluster_name,location,sku_name,capacity,tier,credentials,subscription_id,database_name,principal_id,tenantId ):
    print(f"begin creating ADX cluster {cluster_name} at {location} with {sku_name} and capacity {capacity}")

    cluster = Cluster(location=location, sku=AzureSku(name=sku_name, capacity=capacity, tier=tier),enable_streaming_ingest=True, )

    kusto_management_client = KustoManagementClient(credentials, subscription_id)

    cluster_operations = kusto_management_client.clusters

    poller = cluster_operations.begin_create_or_update(resource_group_name, cluster_name, cluster)
    poller.wait()
    print(f"finished creating cluster {cluster_name}")
    print(f"Beging enabling Python for {cluster_name}")

    poller = cluster_operations.begin_add_language_extensions(resource_group_name, cluster_name, LanguageExtensionsList(value=[LanguageExtension(language_extension_name="PYTHON")]))
    poller.wait()
    print(f"Finished enabling Python for {cluster_name}")

    soft_delete_period = timedelta(days=3650)
    hot_cache_period = timedelta(days=3650)

    print(f"begin creating DB {database_name} for cluster {cluster_name}")

    database_operations = kusto_management_client.databases
    database = ReadWriteDatabase(location=location,
                        soft_delete_period=soft_delete_period,
                        hot_cache_period=hot_cache_period)

    poller = database_operations.begin_create_or_update(resource_group_name = resource_group_name, cluster_name = cluster_name, database_name = database_name, parameters = database)
    poller.wait()
    print(f"finished creating database")
    principal_assignment_name = "clusterPrincipalAssignment1"
    #User email, application ID, or security group name
    #AllDatabasesAdmin, AllDatabasesMonitor or AllDatabasesViewer
    role = "Admin"
    tenant_id_for_principal = tenantId
    #User, App, or Group
    principal_type = "App"
    # Returns an instance of LROPoller, check https://docs.microsoft.com/python/api/msrest/msrest.polling.lropoller?view=azure-python
    try:
        poller = kusto_management_client.database_principal_assignments.begin_create_or_update(resource_group_name=resource_group_name, cluster_name=cluster_name, database_name=database_name, principal_assignment_name= principal_assignment_name, parameters=DatabasePrincipalAssignment(principal_id=principal_id, role=role, tenant_id=tenant_id_for_principal, principal_type=principal_type))
    except: #handling an error that is not understood. The assignment is still successful.
        pass
    print(f"finished assigning SP to database")

def create_service_principal(sp_name, subscription_id, resource_group_name, keyvault=None):
    cmd = f"az ad sp create-for-rbac --name {sp_name} --role contributor --scopes /subscriptions/{subscription_id}/resourceGroups/{resource_group_name} --sdk-auth"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    print(result)
    result = json.loads(result.stdout.decode('utf-8'))
    if keyvault:
        keyvault.set_secret(name=KV_SP_ID, value = result['clientId'])
        keyvault.set_secret(name=KV_SP_KEY, value = result['clientSecret'])
        keyvault.set_secret(name=KV_TENANT_ID, value = result['tenantId'])
    return result['clientId'], result['clientSecret']

def azlogin(tenant_id):
    cmd = f"az login --tenant {tenant_id}"
    print("if the login screen does not pop-up, please copy and run the following command to login")
    print(cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)

    return result



def provision(ws=None, tenant_id =None, location=None, client_id = None, client_secret=None, subscription_id=None,resource_group_name=None,cluster_name=None, database_name ="mlmonitoring",sku_name = 'Standard_D11_v2', tier = "Standard",capacity = 2):
    

    """This method create service principal, setup ADX cluster and store credentials in the Azure ML's default keyvault.
 
    Input Arguments: ws=None, tenant_id =None, location=None, client_id = None, client_secret=None, subscription_id=None,resource_group_name=None,cluster_name=None, database_name ="mlmonitoring",sku_name = 'Dev(No SLA)_Standard_D11_v2', tier = "Basic",capacity = 1
    SKU name. Possible values include: "Dev(No SLA)_Standard_D11_v2", "Dev(No SLA)_Standard_E2a_v4", "Standard_D11_v2", "Standard_D12_v2", "Standard_D13_v2", "Standard_D14_v2", "Standard_D32d_v4", "Standard_D16d_v5", "Standard_D32d_v5", "Standard_DS13_v2+1TB_PS", "Standard_DS13_v2+2TB_PS", "Standard_DS14_v2+3TB_PS", "Standard_DS14_v2+4TB_PS", "Standard_L4s", "Standard_L8s", "Standard_L16s", "Standard_L8s_v2", "Standard_L16s_v2", "Standard_E64i_v3", "Standard_E80ids_v4", "Standard_E2a_v4", "Standard_E4a_v4", "Standard_E8a_v4", "Standard_E16a_v4", "Standard_E8as_v4+1TB_PS", "Standard_E8as_v4+2TB_PS", "Standard_E16as_v4+3TB_PS", "Standard_E16as_v4+4TB_PS", "Standard_E8as_v5+1TB_PS", "Standard_E8as_v5+2TB_PS", "Standard_E16as_v5+3TB_PS", "Standard_E16as_v5+4TB_PS", "Standard_E2ads_v5", "Standard_E4ads_v5", "Standard_E8ads_v5", "Standard_E16ads_v5", "Standard_E8s_v4+1TB_PS", "Standard_E8s_v4+2TB_PS", "Standard_E16s_v4+3TB_PS", "Standard_E16s_v4+4TB_PS", "Standard_E8s_v5+1TB_PS", "Standard_E8s_v5+2TB_PS", "Standard_E16s_v5+3TB_PS", "Standard_E16s_v5+4TB_PS".
    SKU tier. Possible values include: "Basic", "Standard".


    """    
    

    kv =None
    create_standalone_cluster =True
    if ws:
        kv = ws.get_default_keyvault()
        ws_detail = ws.get_details()
        ws_name = ws_detail['name']
        if cluster_name is None:
            cluster_name = ws_name + "monitor"+ str(random.randint(0,99))
            cluster_name= cluster_name.replace("_","").replace("-","")[:22].lower() #to follow ADX's cluster naming convention
            create_standalone_cluster = False
        tenant_id = ws_detail['identity']['tenant_id']
        location = ws_detail['location']
        kv.set_secret(name=KV_ADX_URI, value = f"https://{cluster_name}.{location}.kusto.windows.net")
        subscription_id = ws_detail['id'].split("/")[2]
        resource_group_name = ws_detail['id'].split("/")[4]
        kv.set_secret(name=KV_ADX_DB, value = database_name)
        azlogin(tenant_id)

    if client_id is None:
        sp_name =ws_name+"_"+cluster_name+"_"+ SP_NAME_PF
        print("Creating Service Principal")
        client_id,client_secret= create_service_principal(sp_name, subscription_id, resource_group_name, kv)

        credentials = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id
        )
        time.sleep(120) #wait for the SP to become active
    if create_standalone_cluster:
        if cluster_name is None:
            raise Exception("You need to supply cluster_name, resource_group_name, subscription_id values to create ADX cluster")
        create_adx_cluster(resource_group_name, cluster_name,location,sku_name,capacity,tier,credentials,subscription_id,database_name,client_id,tenant_id )

    else:
        create_adx_cluster(resource_group_name, cluster_name,location,sku_name,capacity,tier,credentials,subscription_id,database_name,client_id,tenant_id )

def set_adx_to_workspace(ws, cluster_uri,db_name, client_id, client_secret, subscription_id=None, tenant_id =None):

    #Assign a existing ADX cluster to the workspace 
    kv = ws.get_default_keyvault()

    ws_detail = ws.get_details()
    if tenant_id is None:
        tenant_id = ws_detail['identity']['tenant_id']
    if subscription_id is None:
        subscription_id = ws_detail['id'].split("/")[2]
    kv.set_secret(KV_SP_ID,client_id)
    kv.set_secret(KV_SP_KEY,client_secret)
    kv.set_secret(KV_ADX_DB,db_name)
    kv.set_secret(KV_ADX_URI,cluster_uri)
    kv.set_secret(KV_TENANT_ID,tenant_id)