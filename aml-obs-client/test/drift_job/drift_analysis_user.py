import concurrent.futures
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from obs.drift.core import Drift_Analysis

class Drift_Analysis_User(Drift_Analysis):
    def analyze_drift(self,base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit=None, concurrent_run=True, drift_threshold =0.5):
        if limit is None:
            limit = ""
        else:
            limit = f"| limit {limit}"
        base_tbl_columns = self.list_table_columns(base_table_name)
        target_tbl_columns = self.list_table_columns(target_table_name)
        common_columns = base_tbl_columns.merge(target_tbl_columns)
        timestamp_cols = common_columns[common_columns['AttributeType']=='DateTime']
        if timestamp_cols.shape[0]==0:
            raise Exception("No timestamp column found! ")
        if "timestamp" in timestamp_cols['AttributeName'].values:
            time_stamp_col ='timestamp'
        else:
            time_stamp_col = timestamp_cols['AttributeName'].values[0]
        numerical_columns = common_columns[(common_columns['AttributeType']!='DateTime')&(common_columns['AttributeType']!='StringBuffer')]
        numerical_columns = numerical_columns['AttributeName'].values
        categorical_columns = common_columns[common_columns['AttributeType']=='StringBuffer']['AttributeName'].values
        if concurrent_run:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                categorical_output_future = executor.submit(self.analyze_drift_categorical, categorical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to,bin, limit)
                numberical_output_future = executor.submit(self.analyze_drift_numerical,numerical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit)
                categorical_output = categorical_output_future.result()
                numberical_output = numberical_output_future.result()
        else:
            categorical_output =self.analyze_drift_categorical(categorical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to,bin, limit)
            numberical_output = self.analyze_drift_numerical(numerical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit)
        scaler = MinMaxScaler()
        categorical_output.sort_values(['categorical_feature', 'target_start_date'], inplace=True)
        categorical_output['scaled_metric']= categorical_output.groupby(['categorical_feature']).apply(lambda x:scaler.fit_transform(np.array(x.euclidean.values).reshape(-1,1)).flatten()).explode().values
        numberical_output.sort_values(['numeric_feature', 'target_start_date'], inplace=True)
        numberical_output['scaled_metric']= numberical_output.groupby(['numeric_feature']).apply(lambda x:scaler.fit_transform(np.array(x.wasserstein.values).reshape(-1,1)).flatten()).explode().values
        output = pd.concat([categorical_output, numberical_output])
        drift_result = output.groupby("target_start_date")['scaled_metric'].mean()>drift_threshold

        return output,drift_result
    def analyze_drift_v2(self,base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit=None, concurrent_run=True):
        if limit is None:
            limit = ""
        else:
            limit = f"| limit {limit}"

        base_tbl_columns = self.list_table_columns(base_table_name)
        target_tbl_columns = self.list_table_columns(target_table_name)
        common_columns = base_tbl_columns.merge(target_tbl_columns)
        timestamp_cols = common_columns[common_columns['AttributeType']=='DateTime']
        if timestamp_cols.shape[0]==0:
            raise Exception("No timestamp column found! ")
        if "timestamp" in timestamp_cols['AttributeName'].values:
            time_stamp_col ='timestamp'
        else:
            time_stamp_col = timestamp_cols['AttributeName'].values[0]
        numerical_columns = common_columns[(common_columns['AttributeType']!='DateTime')&(common_columns['AttributeType']!='StringBuffer')]
        numerical_columns = numerical_columns['AttributeName'].values
        categorical_columns = common_columns[common_columns['AttributeType']=='StringBuffer']['AttributeName'].values

        if concurrent_run:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                categorical_output_futures=[]
                numberical_output_futures =[]

                for cat_feature in categorical_columns:
                    categorical_output_future = executor.submit(self.analyze_drift_categorical, [cat_feature], time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to,bin, limit)
                    categorical_output_futures.append(categorical_output_future)

                for num_feature in numerical_columns:
                    numberical_output_future = executor.submit(self.analyze_drift_numerical,[num_feature], time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit)
                    numberical_output_futures.append(numberical_output_future)

                categorical_output = pd.concat([categorical_output_future.result() for categorical_output_future in categorical_output_futures])
                numberical_output =  pd.concat([numberical_output_future.result() for numberical_output_future in numberical_output_futures]) 
        else:
            categorical_output =self.analyze_drift_categorical(categorical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to,bin, limit)
            numberical_output = self.analyze_drift_numerical(numerical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit)



        output =numberical_output.merge(categorical_output, how="outer", on = "target_start_date")
        for metric in ['wasserstein', 'base_min', 'base_max','base_mean','target_min', 'target_max','target_mean', 'euclidean','base_dcount','target_dcount']:
            output[metric]= output[metric].astype("float")
        return output
    def analyze_drift_categorical(self,categorical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit=None):
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
let target = 
{target_table_name}
| where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}') 
{limit}
| project ['{time_stamp_col}'], {cat_feature_list}, properties = pack_all()
| mv-apply categorical_feature = categorical_features to typeof(string) on (
    project categorical_feature, categorical_feature_value = tostring(properties[categorical_feature])
)
| project-away properties, {cat_feature_list}
| summarize target_categorical_feature_value = make_list(categorical_feature_value), target_dcount= dcount(categorical_feature_value) by target_start_date=bin(['{time_stamp_col}'],{bin}),categorical_feature
| where array_length(target_categorical_feature_value)>0;
{base_table_name}
| where ['{time_stamp_col}'] >= datetime('{base_dt_from}') and ['{time_stamp_col}'] <= datetime('{base_dt_to}') 
{limit}
| project ['{time_stamp_col}'], {cat_feature_list}, properties = pack_all()
| mv-apply categorical_feature = categorical_features to typeof(string) on (
    project categorical_feature, categorical_feature_value = tostring(properties[categorical_feature])
)
| project-away properties, {cat_feature_list}
| summarize base_categorical_feature_value = make_list(categorical_feature_value), base_dcount= dcount(categorical_feature_value) by categorical_feature
|where array_length(base_categorical_feature_value)>0
|join target on categorical_feature
| evaluate hint.distribution = per_node python(
//
typeof(*, euclidean:double),               //  Output schema: append a new fx column to original table 
```
from scipy.spatial import distance
from sklearn import preprocessing
import random
import numpy as np
result = df
n = df.shape[0]
euc_distance =[]
le = preprocessing.LabelEncoder()

for i in range(n):
    base_features = df["base_categorical_feature_value"][i]
    target_features = df["target_categorical_feature_value"][i]
    le.fit(base_features+target_features)
    if len(target_features) > len(base_features):
        target_features = random.sample(target_features, len(base_features))
    elif len(target_features) < len(base_features):
        base_features = random.sample(base_features, len(target_features))
    base_features.sort()
    target_features.sort()
    euc_distance.append(distance.euclidean(le.transform(base_features), le.transform(target_features)))
result['euclidean'] =euc_distance

```
)
|extend target_end_date = target_start_date+ {bin}
|project target_start_date, target_end_date, categorical_feature,euclidean,  base_dcount,target_dcount
        
        """
        # print(query)
        return self.query(query)

    def analyze_drift_numerical(self,numerical_columns, time_stamp_col, base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit=None):
        num_feature_list = ""
        num_feature_list_with_quote =""
        for feature in numerical_columns:
            num_feature_list_with_quote = num_feature_list_with_quote+f"'{feature}'"+","
        num_feature_list_with_quote = num_feature_list_with_quote[:-1]
        for feature in numerical_columns:
            num_feature_list = num_feature_list+f"['{feature}']"+","
        num_feature_list = num_feature_list[:-1]
        query = f"""
let numeric_features = dynamic([{num_feature_list_with_quote}]);
let target = 
{target_table_name}
| where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}') 
{limit}
| project ['{time_stamp_col}'], {num_feature_list}, properties = pack_all()
| mv-apply numeric_feature = numeric_features to typeof(string) on (
    project numeric_feature, numeric_feature_value = todouble(properties[numeric_feature])
)
| project-away properties, {num_feature_list}
| summarize  target_numeric_feature_value = make_list(numeric_feature_value), target_min = min(numeric_feature_value), target_max= max(numeric_feature_value), target_mean =percentiles(numeric_feature_value,50) by target_start_date=bin(['{time_stamp_col}'],{bin}), numeric_feature
| where array_length(target_numeric_feature_value)>0;
{base_table_name}
| where ['{time_stamp_col}'] >= datetime('{base_dt_from}') and ['{time_stamp_col}'] <= datetime('{base_dt_to}') 
{limit}
| project {num_feature_list}, properties = pack_all()
| mv-apply numeric_feature = numeric_features to typeof(string) on (
    project numeric_feature, numeric_feature_value = todouble(properties[numeric_feature])
)
| project-away properties, {num_feature_list}
| summarize  base_numeric_feature_value = make_list(numeric_feature_value), base_min = min(numeric_feature_value), base_max= max(numeric_feature_value), base_mean =percentiles(numeric_feature_value,50) by numeric_feature
|where array_length(base_numeric_feature_value)>0
|join target on numeric_feature
| evaluate hint.distribution = per_node python(
//
typeof(*, wasserstein:double),               //  Output schema: append a new fx column to original table 
```
from scipy.stats import wasserstein_distance
#from scipy.special import kl_div
from scipy.spatial import distance
import numpy as np
result = df
n = df.shape[0]
distance1=[]
distance2 =[]
for i in range(n):
    distance1.append(wasserstein_distance(df['base_numeric_feature_value'][i], df['target_numeric_feature_value'][i]))

result['wasserstein'] =distance1

```
)
|extend target_end_date = target_start_date+ {bin}
|project target_start_date,target_end_date, numeric_feature, wasserstein, base_min, base_max,base_mean,target_min, target_max,target_mean

"""
        # print(query)

        return self.query(query)
    def get_categorical_columns_distribution(self,categorical_columns, time_stamp_col, target_table_name, target_dt_from, target_dt_to, bin):
        # print("using custom categorical distribution")

        cat_feature_list = ""
        cat_feature_list_with_quote =""
        for feature in categorical_columns:
            cat_feature_list_with_quote = cat_feature_list_with_quote+f"'{feature}'"+","
        cat_feature_list_with_quote = cat_feature_list_with_quote[:-1]
        for feature in categorical_columns:
            cat_feature_list = cat_feature_list+f"['{feature}']"+","
        cat_feature_list = cat_feature_list[:-1]
        if bin:
            query =f"""
    let categorical_features = dynamic([{cat_feature_list_with_quote}]);
    {target_table_name}
    | where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}') 
    | project ['{time_stamp_col}'], {cat_feature_list}, properties = pack_all()
    | mv-apply feature = categorical_features to typeof(string) on (
        project feature, categorical_feature_value = tostring(properties[feature])
    )
    |summarize count = count() by feature, categorical_feature_value, bin(['{time_stamp_col}'],{bin})
    |summarize value_list= tostring(make_list(categorical_feature_value)), count_list = make_list(['count']) by ['{time_stamp_col}'], feature
    """
        else:
            query =f"""
    let categorical_features = dynamic([{cat_feature_list_with_quote}]);
    {target_table_name}
    | where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}') 
    | project ['{time_stamp_col}'], {cat_feature_list}, properties = pack_all()
    | mv-apply feature = categorical_features to typeof(string) on (
        project feature, categorical_feature_value = tostring(properties[feature])
    )
    |summarize count = count() by feature, categorical_feature_value
    |summarize value_list= tostring(make_list(categorical_feature_value)), count_list = make_list(['count']) by feature| extend ['{time_stamp_col}']= datetime('{target_dt_from}')
    """
        # print(query)
        return self.query(query)
    
    def get_numerical_column_distribution(self, numerical_column, time_stamp_col,target_table_name, target_dt_from, target_dt_to, bin):
        # print("using custom numerical distribution")
        numerical_column = f"['{numerical_column}']"
        if bin:
            query = f"""
    let tbl = {target_table_name}| where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}');
    let bin_range = toscalar(tbl|summarize (max({numerical_column})- min({numerical_column})));
    let num_bin = min_of(bin_range, 50);
    let bin_size_temp = bin_range/num_bin;
    tbl|summarize count = count() by bin({numerical_column},bin_size_temp), bin(['{time_stamp_col}'],{bin})
    | sort by {numerical_column} asc | project ['count'],round({numerical_column}),['{time_stamp_col}']
    | summarize value_list= make_list({numerical_column}), count_list = make_list(['count']) by ['{time_stamp_col}']
            """
        else: #in case this is to produce distribution for the base then bin is not needed

            query = f"""
    let tbl = {target_table_name}| where ['{time_stamp_col}'] >= datetime('{target_dt_from}') and ['{time_stamp_col}'] <= datetime('{target_dt_to}');
    let bin_range = toscalar(tbl|summarize (max({numerical_column})- min({numerical_column})));
    let num_bin = min_of(bin_range, 50);
    let bin_size_temp = bin_range/num_bin;
    tbl|summarize count = count() by bin({numerical_column},bin_size_temp)
    | sort by {numerical_column} asc | project ['count'],round({numerical_column})
    | summarize value_list= make_list({numerical_column}), count_list = make_list(['count']), ['{time_stamp_col}']= datetime('{target_dt_from}')
            """
        # print(query)

        return self.query(query)
