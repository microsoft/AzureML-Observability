import os
from azure.ai.ml import MLClient, command, Input
from azure.identity import DefaultAzureCredential
from azure.ai.ml.entities import Environment, BuildContext
import inspect
from . import drift_analysis_kusto
from textwrap import dedent
import shutil
from datetime import datetime
from dateutil import tz

from azure.ai.ml.dsl import pipeline
from azure.ai.ml.constants import TimeZone
from azure.ai.ml.entities import (
    CronSchedule,
    RecurrenceSchedule,
    RecurrencePattern,
    ScheduleStatus,
)

# add environment variable to enable private preview feature
import os
os.environ["AZURE_ML_CLI_PRIVATE_FEATURES_ENABLED"] = "true"

def execute(subscription_id,resource_group,workspace, compute_name, base_table_name, 
target_table_name, base_dt_from ,base_dt_to,target_dt_from, target_dt_to,user_defined_module_file=None, 
user_defined_conda_file=None,drift_analysis_job_file=None,cron_schedule=None, experiment_name= "drift-analysis-job", bin="1d", limit=100000, concurrent_run=True, drift_threshold=0.5, drift_result_table="data_drift_result"):
    ml_client = MLClient(
        DefaultAzureCredential(), subscription_id, resource_group, workspace
    )

    os.makedirs(".tmp", exist_ok=True)
    if user_defined_conda_file is None:
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
            - scikit-learn==1.1.1
            - plotly==5.7.0
            - azureml-defaults==1.41.0
            - pandas
            - --extra-index-url https://azuremlsdktestpypi.azureedge.net/sdk-cli-v2
            - azure-ai-ml==0.0.62653692
            - git+https://github.com/microsoft/AzureML-Observability#subdirectory=aml-obs-client
            - git+https://github.com/microsoft/AzureML-Observability#subdirectory=aml-obs-collector
        - matplotlib
        - pip < 20.3
        name: drift_detection
        """
        with open(".tmp/conda.yml", "w") as conda_file:
            conda_file.write(dedent(conda_file_content))
    else:
        shutil.copy(user_defined_conda_file,".tmp/conda.yml")
    if user_defined_module_file is not None:
        shutil.copy(user_defined_module_file,".tmp/drift_analysis.py")
    else:
        content = inspect.getsource(drift_analysis_kusto)
        with open(".tmp/drift_analysis.py", "w") as drift_analysis_file:
            drift_analysis_file.write(content)
    if drift_analysis_job_file is not None:
        shutil.copy(drift_analysis_job_file,".tmp/job_file.py")
    else:
        content = """
from obs.drift.drift_analysis_kusto import Drift_Analysis_User
from obs.collector import Online_Collector
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
    parser.add_argument("--concurrent_run", type=bool, default=False)
    parser.add_argument("--drift_threshold", type=float, default=0.5)


    # parse args
    args = parser.parse_args()

    # return args
    return args
def main(args):
    gmt = time.gmtime()
    ts = calendar.timegm(gmt)
    run_id = args.base_table_name+"_"+args.target_table_name+"_"+ str(ts)
    drift_analysis =Drift_Analysis_User()

    df_output, drift_result = drift_analysis.analyze_drift(limit=args.limit,base_table_name = args.base_table_name,target_table_name=args.target_table_name, base_dt_from=args.base_dt_from, base_dt_to=args.base_dt_to, target_dt_from=args.target_dt_from, target_dt_to=args.target_dt_to, bin=args.bin, concurrent_run=args.concurrent_run, drift_threshold = args.drift_threshold)
    print(drift_result)
    df_output['run_id'] = run_id
    df_output['base_start_date']=pd.to_datetime(args.base_dt_from)
    df_output['base_end_date']=pd.to_datetime(args.base_dt_to)
    data_drift_collector = Online_Collector(args.drift_result_table)
    data_drift_collector.batch_collect(df_output)
    feature_distribution = drift_analysis.get_features_distributions(target_table_name=args.target_table_name, target_dt_from=args.target_dt_from, target_dt_to=args.target_dt_to, bin=args.bin)
    feature_distribution['run_id'] = run_id
    feature_distribution_base = drift_analysis.get_features_distributions(target_table_name=args.base_table_name, target_dt_from=args.base_dt_from, target_dt_to=args.base_dt_to, bin=None)
    feature_distribution_base['run_id'] = run_id
    feature_ditriction_collector = Online_Collector(args.feature_distribution_table)
    feature_ditriction_collector.batch_collect(feature_distribution)
    feature_ditriction_collector.batch_collect(feature_distribution_base)

if __name__ == "__main__":
    # parse args
    args = parse_args()

    # run main function
    main(args)
        
        """
        with open(".tmp/job_file.py", "w") as job_file:
            job_file.write(content)


    env_docker_conda = Environment(
        image="mcr.microsoft.com/azureml/openmpi3.1.2-ubuntu18.04",
        conda_file=".tmp/conda.yml",
        name="drift_analysis_job",
        description="Environment created from a Docker image plus Conda environment.",
    )
    job = command(
    code=".tmp",  # local path where the code is stored
    command="python job_file.py --base_table_name ${{inputs.base_table_name}} --target_table_name ${{inputs.target_table_name}} --base_dt_from ${{inputs.base_dt_from}} --base_dt_to ${{inputs.base_dt_to}} --target_dt_from ${{inputs.target_dt_from}} --target_dt_to ${{inputs.target_dt_to}} --bin ${{inputs.bin}} --limit ${{inputs.limit}} --drift_threshold ${{inputs.drift_threshold}} --drift_result_table ${{inputs.drift_result_table}}",
    inputs={"base_table_name": base_table_name, "target_table_name": target_table_name, "base_dt_from":base_dt_from, "base_dt_to": base_dt_to,"target_dt_from": target_dt_from, "target_dt_to":target_dt_to, "bin":bin, "limit":limit, "concurrent_run":concurrent_run,  "drift_threshold":drift_threshold, "drift_result_table":drift_result_table},
    environment=env_docker_conda,
    compute=compute_name,
    display_name=experiment_name    # description,
    
    )
    @pipeline(description="scheduled_drift_analysis")
    def schedule_pipeline():
        job()
    pipeline_job = schedule_pipeline()
    pipeline_job.settings.default_compute=compute_name
    if cron_schedule is not None:
        # create a cron schedule start from current time and fire at minute 0,10 of every hour with PACIFIC STANDARD TIME timezone
        pipeline_job.schedule = cron_schedule


    ml_client.jobs.create_or_update(pipeline_job,experiment_name=experiment_name)
    shutil.rmtree(".tmp")

    return ml_client, pipeline_job.name