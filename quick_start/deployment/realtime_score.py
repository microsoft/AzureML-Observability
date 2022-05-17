
import os
import logging
import json
import joblib
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import Workspace
from monitoring.data_collector import Online_Collector
import datetime
import pandas as pd
from sklearn.preprocessing import LabelEncoder

def init():
    """
    This function is called when the container is initialized/started, typically after create/update of the deployment.
    You can write the logic here to perform init operations like caching the model in memory
    """
    global model,collector, encoder
    # AZUREML_MODEL_DIR is an environment variable created during deployment.
    # It is the path to the model folder (./azureml-models/$MODEL_NAME/$VERSION)
    labels = ['setosa', 'versicolor', 'virginica']
    encoder = LabelEncoder()
    encoder.fit(labels)
    model_path = os.path.join(
        os.getenv("AZUREML_MODEL_DIR"), "model.joblib"
    )
    # deserialize the model file back into a sklearn model
    model = joblib.load(model_path)
    table_name= os.environ.get("TABLE_NAME")
    tenant_id =os.environ.get("TENANT_ID")
    subscription_id = os.environ.get("SUBSCRIPTION_ID")
    client_secret = os.environ.get("SP_SECRET")
    client_id = os.environ.get("SP_ID")
    ws_name = os.environ.get("WS_NAME")
    rg = os.environ.get("RG")

    # cluster_uri = os.environ.get("CLUSTER_URI")
    # database_name = os.environ.get("DATABASE_NAME")
    sp = ServicePrincipalAuthentication(tenant_id=tenant_id, # tenantID
                                    service_principal_id=client_id, # clientId
                                    service_principal_password=client_secret) # clientSecret

    ws = Workspace.get(name=ws_name,
                   auth=sp,
                   subscription_id=subscription_id,
                   resource_group=rg)
    collector = Online_Collector(table_name,ws)
    collector.start_logging_daemon(buffer_time=2, batch_size=10)
    logging.info("Init complete")


def run(raw_data):
    """
    This function is called for every invocation of the endpoint to perform the actual scoring/prediction.
    In the example we extract the data from the json input and call the scikit-learn model's predict()
    method and return the result back
    """
    ts =datetime.datetime.now()

    logging.info("model 1: request received")
    data = pd.DataFrame(json.loads(raw_data)["data"])
    predictions = model.predict(data)
    predictions= encoder.inverse_transform(predictions)
    probs = model.predict_proba(data)
    probs=probs.tolist()
    data["predictions"] =predictions
    data["probs"] =probs
    data['timestamp'] = ts
    data['scoring_service'] = "managed_online"
    logging.info("Request processed")
    collector.stream_collect_df_queue(data)
    return predictions.tolist()

