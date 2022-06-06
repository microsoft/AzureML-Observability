import sys
import os
from drift_analysis import Drift_Analysis_User
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


    # parse args
    args = parser.parse_args()

    # return args
    return args
def main(args):
    gmt = time.gmtime()
    ts = calendar.timegm(gmt)
    run_id = args.base_table_name+"_"+args.target_table_name+"_"+ str(ts)
    drift_analysis =Drift_Analysis_User()

    df_output = drift_analysis.analyze_drift(limit=args.limit,base_table_name = args.base_table_name,target_table_name=args.target_table_name, base_dt_from=args.base_dt_from, base_dt_to=args.base_dt_to, target_dt_from=args.target_dt_from, target_dt_to=args.target_dt_to, bin=args.bin, concurrent_run=args.concurrent_run)
    df_output['run_id'] = run_id
    df_output['base_start_date']=pd.to_datetime(args.base_dt_from)
    df_output['base_end_date']=pd.to_datetime(args.base_dt_to)
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