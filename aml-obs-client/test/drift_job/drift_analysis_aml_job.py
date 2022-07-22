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
    parser.add_argument("--target_dt_from", type=str) #initial target_dt_from
    parser.add_argument("--target_dt_to", type=str)  #initial target_dt_to
    parser.add_argument("--target_dt_shift_step_size", type=str, default="None")
    parser.add_argument("--bin", type=str, default="7d")
    parser.add_argument("--limit", type=str, default="100000")
    parser.add_argument("--drift_result_table", type=str, default="drift_analysis_result")
    parser.add_argument("--feature_distribution_table", type=str, default="feature_distribution")
    parser.add_argument("--concurrent_run", type=bool, default=False)
    parser.add_argument("--drift_threshold", type=float, default=0.5)

    #if this is a scheduled run (target_dt_shift_step_size is provided and cron schedule is provided), the job will take last target_dt_to from result table


    # parse args
    args = parser.parse_args()

    # return args
    return args
def main(args):
    gmt = time.gmtime()
    ts = calendar.timegm(gmt)
    run_prefix= args.base_table_name+"_"+args.target_table_name+"_"
    run_id =run_prefix+ str(ts)
    drift_analysis =Drift_Analysis_User()
    #Check if last run exists for pair of table to increase
    target_dt_from = args.target_dt_from
    target_dt_to= args.target_dt_to
    if args.target_dt_shift_step_size != "None":
        last_run = drift_analysis.query("let last_run_id = "+args.drift_result_table+"|where run_id like '"+run_prefix+"'| summarize max_run = max(run_id); "+args.drift_result_table+"|where run_id == toscalar(last_run_id)")     
        if last_run.shape[0]>0:
            target_dt_from = max(last_run["target_end_date"])
            print("new target_dt_from ", target_dt_from)
            target_dt_to= pd.date_range(target_dt_from,periods=2,freq=args.target_dt_shift_step_size).format()[-1]
            print("new target_dt_to ", target_dt_to)

    df_output, drift_result = drift_analysis.analyze_drift(limit=args.limit,base_table_name = args.base_table_name,target_table_name=args.target_table_name, base_dt_from=args.base_dt_from, base_dt_to=args.base_dt_to, target_dt_from=target_dt_from, target_dt_to=target_dt_to, bin=args.bin, concurrent_run=args.concurrent_run, drift_threshold = args.drift_threshold)
    print(drift_result)
    df_output['run_id'] = run_id
    df_output['base_start_date']=pd.to_datetime(args.base_dt_from)
    df_output['base_end_date']=pd.to_datetime(args.base_dt_to)
    data_drift_collector = Online_Collector(args.drift_result_table)
    data_drift_collector.batch_collect(df_output)
    feature_distribution = drift_analysis.get_features_distributions(target_table_name=args.target_table_name, target_dt_from=target_dt_from, target_dt_to=target_dt_to, bin=args.bin)
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