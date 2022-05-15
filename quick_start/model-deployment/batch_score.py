from PIL import Image
import os
import tempfile
import logging
from monitoring.data_collector import Online_Collector
from azureml.core.model import Model
import joblib
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import os

def init():
    global model,collector, encoder

    labels = ['setosa', 'versicolor', 'virginica']
    encoder = LabelEncoder()
    encoder.fit(labels)
    model_path = os.path.join(
        os.getenv("AZUREML_MODEL_DIR"), "model.joblib"
    )
    model = joblib.load(model_path)


def run(mini_batch):
    print(f"run method start: {__file__}, run({mini_batch})")
    resultList = []
    table_name = os.getenv("TABLE_NAME")
    
    # Set up logging

    for batch in mini_batch:
        # prepare each image
        data = pd.read_parquet(batch)
        predictions = model.predict(data)
        data["prediction"] =predictions

        resultList.append(data)
    result = pd.concat(resultList).drop(['Unnamed: 0'], axis=1)
    online_collector = Online_Collector(table_name)
    online_collector.batch_collect(result)
    return result
