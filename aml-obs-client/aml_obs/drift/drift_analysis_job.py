import os
from azure.ml import MLClient
from azure.ml import command, Input
from azure.identity import DefaultAzureCredential
from azure.ml.entities import Environment, BuildContext
from textwrap import dedent
import shutil


def execute_drift_detect_job(subscription_id="0e9bace8-7a81-4922-83b5-d995ff706507",resource_group="azureml",workspace="ws01ent", compute_name ='DS11', experiment_name= "drift-analysis-job", base_table_name ="ISDWeather", 
target_table_name ="ISDWeather", base_dt_from ="2013-04-13", base_dt_to= "2014-05-13",target_dt_from="2013-04-13", target_dt_to="2014-05-13", bin="7d", limit=3000000):

    ml_client = MLClient(
        DefaultAzureCredential(), subscription_id, resource_group, workspace
    )

    os.makedirs(".tmp", exist_ok=True)
    conda_file_content= f"""
    channels:
    - anaconda
    - conda-forge
    dependencies:
    - python=3.8.1
    - pip:
        - azureml-mlflow==1.41.0
        - azure-identity==1.9.0
        - azure-identity==1.9.0
        - azure-mgmt-kusto==2.2.0
        - azure-kusto-data==3.1.2
        - azure-kusto-ingest==3.1.2
        - dash==2.3.1
        - plotly==5.7.0
        - azureml-defaults==1.41.0
        - pandas
        - --extra-index-url https://azuremlsdktestpypi.azureedge.net/sdk-cli-v2
        - azure-ml==0.0.61212840
        - git+https://github.com/microsoft/MLOpsTemplate.git@monitoring-main#subdirectory=src/utilities
    - matplotlib
    - pip < 20.3
    name: drift_detection
    """

    source_file_content="""
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__),'../'))
    from monitoring.drift_analysis import Drift_Analysis
    from monitoring.data_collector import Online_Collector
    import calendar;
    import time;
    import argparse
    import pandas as pd
    def parse_args():
        # setup arg parser
        parser = argparse.ArgumentParser()

        parser.add_argument("--base_table_name", type=str)
        parser.add_argument("--target_table_name", type=str)
        parser.add_argument("--base_dt_from", type=str)
        parser.add_argument("--base_dt_to", type=str)
        parser.add_argument("--target_dt_from", type=str)
        parser.add_argument("--target_dt_to", type=str)
        parser.add_argument("--bin", type=str, default="7d")
        parser.add_argument("--limit", type=str, default="100000")
        parser.add_argument("--drift_result_table", type=str, default="data_drift_result")
        parser.add_argument("--feature_distribution_table", type=str, default="feature_distribution")


        # parse args
        args = parser.parse_args()

        # return args
        return args
    def main(args):
        gmt = time.gmtime()
        ts = calendar.timegm(gmt)
        run_id = args.base_table_name+"_"+args.target_table_name+"_"+ str(ts)
        drift_analysis =Drift_Analysis()

        df_output = drift_analysis.analyze_drift(limit=args.limit,base_table_name = args.base_table_name,target_table_name=args.target_table_name, base_dt_from=args.base_dt_from, base_dt_to=args.base_dt_to, target_dt_from=args.target_dt_from, target_dt_to=args.target_dt_to, bin=args.bin)
        df_output['base_start_date']=pd.to_datetime(args.base_dt_from)
        df_output['base_end_date']=pd.to_datetime(args.base_dt_to)
        # df_output['target_start_date']=pd.to_datetime(df_output['target_start_date'])
        # df_output['target_end_date']=pd.to_datetime(df_output['target_end_date'])
        df_output.drop(['target_end_date_x','target_end_date_y'], axis =1)
        df_output['run_id'] = run_id
        for metric in ['wasserstein', 'base_min', 'base_max','base_mean','target_min', 'target_max','target_mean', 'euclidean','base_dcount','target_dcount']:
            df_output[metric]= df_output[metric].astype("float")
        data_drift_collector = Online_Collector(args.drift_result_table)
        data_drift_collector.batch_collect(df_output)
        feature_distribution = drift_analysis.get_features_distributions(target_table_name=args.target_table_name, target_dt_from=args.target_dt_from, target_dt_to=args.target_dt_to, bin=args.bin)
        feature_distribution['run_id'] = run_id
        feature_ditriction_collector = Online_Collector(args.feature_distribution_table)
        feature_ditriction_collector.batch_collect(feature_distribution)

    if __name__ == "__main__":
        # parse args
        args = parse_args()

        # run main function
        main(args)
        
    """
    source_file = open(".tmp/source_file.py", "w")
    source_file.write(dedent(source_file_content))
    source_file.close()
    conda_file = open(".tmp/conda.yml", "w")
    conda_file.write(dedent(conda_file_content))
    conda_file.close()
    env_docker_conda = Environment(
        image="mcr.microsoft.com/azureml/openmpi3.1.2-ubuntu18.04",
        conda_file=".tmp/conda.yml",
        name="drift_analysis_job",
        description="Environment created from a Docker image plus Conda environment.",
    )
    job = command(
    code=".tmp",  # local path where the code is stored
    command="python source_file.py --base_table_name ${{inputs.base_table_name}} --target_table_name ${{inputs.target_table_name}} --base_dt_from ${{inputs.base_dt_from}} --base_dt_to ${{inputs.base_dt_to}} --target_dt_from ${{inputs.target_dt_from}} --target_dt_to ${{inputs.target_dt_to}} --bin ${{inputs.bin}} --limit ${{inputs.limit}}",
    inputs={"base_table_name": base_table_name, "target_table_name": target_table_name, "base_dt_from":base_dt_from, "base_dt_to": base_dt_to,"target_dt_from": target_dt_from, "target_dt_to":target_dt_to, "bin":bin, "limit":limit},
    environment=env_docker_conda,
    compute=compute_name,
    display_name=experiment_name,
    experiment_name= experiment_name
    # description,
    
    )

    returned_job = ml_client.create_or_update(job)
    shutil.rmtree(".tmp")
    