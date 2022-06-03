from azure.kusto.data import KustoClient, KustoConnectionStringBuilder,ClientRequestProperties
from azure.kusto.data.helpers import dataframe_from_result_table
from obs import KV_SP_ID, KV_SP_KEY, KV_ADX_DB, KV_ADX_URI, KV_TENANT_ID
import concurrent.futures
from datetime import timedelta
import pandas as pd


class Drift_Analysis():
    def __init__(self,ws=None,tenant_id=None, client_id=None,client_secret=None,cluster_uri=None,database_name=None):

        if ws is not None:
            kv = ws.get_default_keyvault()
            self.client_id = kv.get_secret(KV_SP_ID)
            self.client_secret = kv.get_secret(KV_SP_KEY)
            self.cluster_uri = kv.get_secret(KV_ADX_URI)
            self.database_name = kv.get_secret(KV_ADX_DB)
            self.tenant_id = kv.get_secret(KV_TENANT_ID)
        elif tenant_id is None: 
            #check if this under AML run
            try:
                from azureml.core import Run
                run = Run.get_context()
                ws = run.experiment.workspace
                kv = ws.get_default_keyvault()
                self.client_id = kv.get_secret(KV_SP_ID)
                self.client_secret = kv.get_secret(KV_SP_KEY)
                self.cluster_uri = kv.get_secret(KV_ADX_URI)
                self.database_name = kv.get_secret(KV_ADX_DB)
                self.tenant_id = kv.get_secret(KV_TENANT_ID)
            except:
                Exception("If not in AML run, need to provide either workspace object or  service principal credential and ADX cluster details")
        else:
            self.tenant_id = tenant_id
            self.client_id = client_id
            self.cluster_uri = cluster_uri
            self.database_name = database_name
            self.client_secret=client_secret
        self.cluster_ingest_uri = self.cluster_uri.split(".")[0][:8]+"ingest-"+self.cluster_uri.split(".")[0].split("//")[1]+"."+".".join(self.cluster_uri.split(".")[1:])
        self.client_req_properties = ClientRequestProperties()
        self.client_req_properties.set_option(self.client_req_properties.no_request_timeout_option_name , True)
        timeout = timedelta(hours=1, seconds=30)
        self.client_req_properties.set_option(self.client_req_properties.request_timeout_option_name , timeout)

        KCSB_DATA = KustoConnectionStringBuilder.with_aad_application_key_authentication(self.cluster_uri, self.client_id, self.client_secret, self.tenant_id)
        self.client = KustoClient(KCSB_DATA)

    def query(self, query):#generic query
        response = self.client.execute(self.database_name, query, self.client_req_properties)
        dataframe = dataframe_from_result_table(response.primary_results[0])
        return dataframe
    def list_tables(self):
        return list(self.query(".show tables")['TableName'])
    def list_table_columns(self, table_name):
        return self.query(f".show table {table_name}")[['AttributeName', 'AttributeType']]
    def get_time_range(self, table_name, timestamp_col=None):
        columns = self.list_table_columns(table_name)
        timestamp_cols = columns[columns['AttributeType']=='DateTime']
        if timestamp_cols.shape[0]==0:
            raise Exception("No timestamp column found! ")
        if timestamp_col is None:
            if "timestamp" in timestamp_cols['AttributeName'].values:
                timestamp_col ='timestamp'
            else:
                timestamp_col = timestamp_cols['AttributeName'].values[0]
        time_range = self.query(f"{table_name} | summarize time_start = min(['{timestamp_col}']), time_end = max(['{timestamp_col}'])")
        return str(time_range['time_start'].values[0]), str(time_range['time_end'].values[0])
    def get_timestamp_col(self, table_name):
        columns = self.list_table_columns(table_name)
        timestamp_cols = columns[columns['AttributeType']=='DateTime']
        if timestamp_cols.shape[0]==0:
            raise Exception("No timestamp column found! ")
        if "timestamp" in timestamp_cols['AttributeName'].values:
            timestamp_col ='timestamp'
        else:
            timestamp_col = timestamp_cols['AttributeName'].values[0]
        return timestamp_col

    def sample_data_points(self, table_name, col_name, start_date, end_date=None, horizon=None, max_rows = 10000):
        timestamp_col = self.get_timestamp_col(table_name)
        if end_date is None:
            end_date = f"datetime_add('day',{horizon}, datetime('{start_date}'))"
        else:
            end_date = f"datetime('{end_date}')"
        query = f"{table_name}|where ['{timestamp_col}'] >= datetime('{start_date}') and ['{timestamp_col}'] <= {end_date}| sample {max_rows}|project {col_name}"
        return self.query(query)

    def get_categorical_columns_distribution(self,categorical_columns, time_stamp_col, target_table_name, target_dt_from, target_dt_to, bin):
        cat_feature_list = ""
        cat_feature_list_with_quote =""
        for feature in categorical_columns:
            cat_feature_list_with_quote = cat_feature_list_with_quote+f"'{feature}'"+","
        cat_feature_list_with_quote = cat_feature_list_with_quote[:-1]
        for feature in categorical_columns:
            cat_feature_list = cat_feature_list+f"['{feature}']"+","
        cat_feature_list = cat_feature_list[:-1]
        query =f"""
let categorical_features = dynamic([{cat_feature_list_with_quote}]);
{target_table_name}
| where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}') 
| project ['{time_stamp_col}'], {cat_feature_list}, properties = pack_all()
| mv-apply categorical_feature = categorical_features to typeof(string) on (
    project categorical_feature, categorical_feature_value = tostring(properties[categorical_feature])
)
|summarize count = count() by categorical_feature, categorical_feature_value, bin(['{time_stamp_col}'],{bin})
|summarize value_list= tostring(make_list(categorical_feature_value)), count_list = make_list(['count']) by ['{time_stamp_col}'],feature =categorical_feature
"""
        # print(query)
        return self.query(query)
    
    def get_numerical_column_distribution(self, numerical_column, time_stamp_col,target_table_name, target_dt_from, target_dt_to, bin):
        numerical_column = f"['{numerical_column}']"
        query = f"""
let tbl = {target_table_name}| where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}');
let bin_range = toscalar(tbl|summarize (max({numerical_column})- min({numerical_column})));
let num_bin = min_of(bin_range, 50);
let bin_size_temp = bin_range/num_bin;
tbl|summarize count = count() by bin({numerical_column},bin_size_temp), bin(['{time_stamp_col}'],{bin})
| summarize value_list= make_list({numerical_column}), count_list = make_list(['count']) by ['{time_stamp_col}']
        """
        # print(query)

        return self.query(query)

    def get_features_distributions(self,target_table_name, target_dt_from, target_dt_to, bin, concurrent_run=True):
        target_tbl_columns = self.list_table_columns(target_table_name)
        timestamp_cols = target_tbl_columns[target_tbl_columns['AttributeType']=='DateTime']
        if timestamp_cols.shape[0]==0:
            raise Exception("No timestamp column found! ")
        if "timestamp" in timestamp_cols['AttributeName'].values:
            time_stamp_col ='timestamp'
        else:
            time_stamp_col = timestamp_cols['AttributeName'].values[0]
        numerical_columns = target_tbl_columns[(target_tbl_columns['AttributeType']!='DateTime')&(target_tbl_columns['AttributeType']!='StringBuffer')]
        numerical_columns = numerical_columns['AttributeName'].values
        categorical_columns = target_tbl_columns[target_tbl_columns['AttributeType']=='StringBuffer']['AttributeName'].values

        if concurrent_run:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                categorical_output=[]
                numberical_output_futures ={}
                numberical_output=[]
                categorical_output_future = executor.submit(self.get_categorical_columns_distribution, categorical_columns, time_stamp_col,target_table_name,  target_dt_from, target_dt_to,bin)
                for num_feature in numerical_columns:
                    numberical_output_future = executor.submit(self.get_numerical_column_distribution,num_feature, time_stamp_col,target_table_name, target_dt_from, target_dt_to, bin)
                    numberical_output_futures[num_feature] = numberical_output_future

                categorical_output = categorical_output_future.result()
                for num_feature in numerical_columns:
                    num_output = numberical_output_futures[num_feature].result()
                    num_output["feature"]=num_feature
                    numberical_output.append(num_output)

                numberical_output =  pd.concat(numberical_output) 

        else:
            categorical_output =self.get_categorical_columns_distribution(categorical_columns, time_stamp_col,target_table_name,  target_dt_from, target_dt_to,bin)

            numberical_output=[]
            for num_feature in numerical_columns:
                feature_output = self.get_numerical_column_distribution(num_feature, time_stamp_col,target_table_name, target_dt_from, target_dt_to, bin)
                feature_output["feature"]=num_feature
                numberical_output.append(feature_output)
            numberical_output =  pd.concat(numberical_output) 

        output =pd.concat([numberical_output, categorical_output])
        return output

    def analyze_drift(self,base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit=None, concurrent_run=True):
        """abstract method for drift detection

        :param base_table_name: base_table_name.
        :type str
        :param target_table_name: target_table_name to compare to.
        :type str
        :base_dt_from date to select data from in base table format: yyyy/mm/dd
        :type str
        :base_dt_to date to select data to in base table format: yyyy/mm/dd
        :type str
        :target_dt_from date to select data from  in base table format: yyyy/mm/dd
        :type str
        :target_dt_to date to select data to in base table format: yyyy/mm/dd
        :type str
        
        """

        pass

