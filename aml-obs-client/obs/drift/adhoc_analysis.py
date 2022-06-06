from jupyter_dash import JupyterDash
import numpy as np
import dash
import pandas as pd
from dateutil import parser
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, State
import plotly.graph_objects as go
from .core import Drift_Analysis
from .drift_analysis_kusto import Drift_Analysis_User
def launch_dashboard(drift_analysis:Drift_Analysis_User):
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = JupyterDash(__name__, external_stylesheets=external_stylesheets)

    tables_list = drift_analysis.list_tables()

    # 'border': '1px solid #bbb'
    app.layout = html.Div([
        html.Div([
            html.Div([
                html.Div([
                    html.H4(
                        children="Base Data Selection",
                        style={"text-align": "center"},
                    ),
                    html.Br(),
                    html.Label("Select Table Name:"),
                    dcc.Dropdown(
                        tables_list,
                        tables_list[0],
                        id='tables',
                    ),
                    html.Br(),
                    html.Label("Select Date Range:"),
                    dcc.DatePickerRange(
                        id='base_timeline',
                        start_date_placeholder_text ="base_start_date",
                        end_date_placeholder_text ="base_end_date"
                    )
                ],
                    style={'padding': 30, 'flex': 1}
                ),
                html.Div([
                    html.H4(
                        children="Target Data Selection",
                        style={"text-align": "center"}
                    ),
                    html.Br(),
                    html.Label("Select Table Name:"),
                    dcc.Dropdown(
                        tables_list,
                        tables_list[0],
                        id='trgt_tables',
                    ),
                    html.Br(),
                    html.Label("Select Date Range:"),
                    dcc.DatePickerRange(
                        id='target_timeline',
                        start_date_placeholder_text ="target_start_date",
                        end_date_placeholder_text ="target_end_date"
                    )
                ],
                    style={'padding': 30, 'flex': 1}
                ),
                html.Div([
                    html.Button('Prepare data', 
                        id='prepare_data', 
                        n_clicks=0,
                        style = {'text-align': 'center', 'border': '1px solid #00ab00'}
                    ),
                    dcc.Store(
                        id='intermediate-value'
                    )
                ], 
                    style={'padding': 40, 'flex': 1}
                )
        ], 
            style={'display': 'flex', 'flex-direction': 'row', 'align-items': 'flex-end', 'justify-content': 'space-between'}
        ),
        dcc.Tabs([
            dcc.Tab(label="Data Overview", children=[            
                    dcc.Graph(id='overview_graph'),
                    dcc.Graph(id='feature_drift_graph')                       
            ]),
            dcc.Tab(label="Feature Drift", children=[
                html.Div([
                    html.Div([
                        html.Label("Select Feature:"),
                        dcc.Dropdown(
                        id='columns'
                        )
                    ],
                        style={'padding': 30, 'flex': 1}
                    ),
                    html.Div([
                        html.Label("Select Metric:"),
                        dcc.Dropdown(
                            id='metrics'
                        ),
                        dcc.Store(
                            id='column-name-type'
                        )
                    ], 
                        style={'padding': 30, 'flex': 1}
                    )
                ],
                style={'display': 'flex', 'flex-direction': 'row', 'align-items': 'right'}
                ),
                html.Div([
                    dcc.Graph(
                        id='graph',
                    ),
                    dcc.Graph(
                        id='detail_graph'
                    )
                ],
                )
            ])
        ])
        ])
    ]
    )

    @app.callback(
        Output('trgt_tables', 'value'),
        Input('tables', 'value'))
    def set_columns_options(table_name):
        return table_name

    @app.callback(
        Output('columns', 'options'),
        Input('tables', 'value'))
    def set_columns_options(table_name):
        return drift_analysis.list_table_columns(table_name)['AttributeName'].values
    @app.callback(
        Output('column-name-type', 'data'),
        Input('tables', 'value'))
    def set_columns_options(table_name):
        return drift_analysis.list_table_columns(table_name).to_json(date_format='iso', orient='split')

    @app.callback(
        Output('columns', 'value'),
        Input('columns', 'options'))
    def set_columns_value(available_options):
        return available_options[0]

    @app.callback(
        Output('metrics', 'options'),
        Input('columns', 'value'),
        Input('column-name-type', 'data'),
    )
    def set_metric_options(column, column_dict):
        column_df = pd.read_json(column_dict, orient='split')
        column_type = str(column_df[column_df['AttributeName']==column]['AttributeType'].values[0])
        if column_type=='StringBuffer':
            return ['dcount', 'euclidean']
        elif column_type=='DateTime':
            return ['NA']
        else:
            return ['max', 'min', "mean", "wasserstein"]
    @app.callback(
        Output('metrics', 'value'),
        Input('metrics', 'options'))
    def set_columns_value(available_options):
        return available_options[0]


    @app.callback(
        [Output('base_timeline', 'min_date_allowed'),Output('base_timeline', 'max_date_allowed'),Output('base_timeline', 'initial_visible_month'),Output('base_timeline', 'start_date'), Output('base_timeline', 'end_date')],
        Input('tables', 'value'))
    def set_columns_options(table_name):
        time_range =drift_analysis.get_time_range(table_name)

        start_date = parser.parse(time_range[0]).replace(microsecond=0, second=0, minute=0)
        end_date = parser.parse(time_range[1]).replace(microsecond=0, second=0, minute=0)
        
        return [start_date, end_date,end_date, start_date, end_date]

    @app.callback(
        [Output('target_timeline', 'min_date_allowed'),Output('target_timeline', 'max_date_allowed'),Output('target_timeline', 'initial_visible_month'),Output('target_timeline', 'start_date'), Output('target_timeline', 'end_date')],
        Input('trgt_tables', 'value'))
    def set_columns_options(table_name):
        time_range =drift_analysis.get_time_range(table_name)

        start_date = parser.parse(time_range[0]).replace(microsecond=0, second=0, minute=0)
        end_date = parser.parse(time_range[1]).replace(microsecond=0, second=0, minute=0)
        
        return [start_date, end_date,end_date, start_date, end_date]

    @app.callback(
        Output('intermediate-value', 'data'),
        State('tables', 'value'),
        State('base_timeline', 'start_date'),
        State('base_timeline', 'end_date'), 
        State('trgt_tables', 'value'),
        State('target_timeline', 'start_date'),
        State('target_timeline', 'end_date'), 
        Input('prepare_data', 'n_clicks'),
            prevent_initial_call=True

        
        )
    def calculate(table_name, base_start_date, base_end_date,target_table_name,target_start_date, target_end_date,n_clicks):
                # self,base_table_name,target_table_name, base_dt_from, base_dt_to, target_dt_from, target_dt_to, bin, limit=10000000, concurrent_run=True)
        query_df = drift_analysis.analyze_drift(limit=100000,base_table_name = f'{table_name}', target_table_name=f'{target_table_name}', base_dt_from=f'{base_start_date}', base_dt_to=f'{base_end_date}', target_dt_from=f'{target_start_date}', target_dt_to=f'{target_end_date}', bin='30d')
        return query_df.to_json(date_format='iso', orient='split')

    @app.callback(
        Output('overview_graph', 'figure'),
        Input('intermediate-value', 'data'),
        prevent_initial_call=True
        )
    def update_drift_figure(jsonified_cleaned_data):
        x_values =[0]
        y_values =[0]
        if jsonified_cleaned_data is not None:
            dff = pd.read_json(jsonified_cleaned_data, orient='split')
            dff.sort_values("target_start_date", inplace=True)
            dff["feature_drift"] = np.where(pd.isna(dff["euclidean"]), dff["wasserstein"]/(dff["target_mean"]+1), dff["euclidean"]/(dff["target_dcount"]+1))
            avgs = dff.groupby("target_start_date")["feature_drift"].mean()
            # x_values = list(avgs.index.strftime("%m/%d/%Y"))
            x_values = list(avgs.index)
            y_values = list(avgs.values)

        fig = go.Figure([go.Scatter(x=x_values, y=y_values)])
        fig.update_layout(title_text="Data Drift", title_x=0.5)

        return fig

    @app.callback(
        Output('graph', 'figure'),
        Input('columns', 'value'),
        Input('metrics', 'value'),
        Input('intermediate-value', 'data'),
        prevent_initial_call=True

        )
    def update_figure(column,metric, jsonified_cleaned_data):
        x_values =[0]
        y_values =[0]
        if jsonified_cleaned_data is not None:
            dff = pd.read_json(jsonified_cleaned_data, orient='split')
            dff.sort_values("target_start_date", inplace=True)
            if metric != "wasserstein" and metric != "euclidean" and metric !='NA':
                metric = "target_"+metric
            if dff[dff['numeric_feature']==column].shape[0]>0:
                y_values = dff[dff['numeric_feature']==column][metric]
                x_values= dff[dff['numeric_feature']==column]["target_start_date"]
            elif dff[dff['categorical_feature']==column].shape[0]>0:
                y_values = dff[dff['categorical_feature']==column][metric]

                x_values = dff[dff['categorical_feature']==column]["target_start_date"]


        fig = go.Figure([go.Scatter(x=x_values, y=y_values, name=f"{column}")])
        fig.update_layout(title_text="Feature Analysis", title_x=0.5)

        return fig
    @app.callback(
        Output('detail_graph', 'figure'),
        Input('graph', 'clickData'),   
        Input('tables', 'value'),
        Input('columns', 'value')
    )
    def drilldown(click_data,table, column):

        # using callback context to check which input was fired
        ctx = dash.callback_context
        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == 'graph':

            if click_data is not None:
                start_date = click_data['points'][0]['x']
                output = drift_analysis.sample_data_points(table,column,start_date,horizon=30)
                output.dropna(inplace=True)
                fig = px.histogram(output,x=column )
                fig.update_layout(title_text="Feature Distribution", title_x=0.5)
                return fig

        else:
            fig = go.Figure(data=[go.Histogram(x=[0] )])
            fig.update_layout(title_text="Feature Distribution", title_x=0.5)
            return fig

    @app.callback(
        Output('feature_drift_graph', 'figure'),
        Input('intermediate-value', 'data'),
        prevent_initial_call=True
        )
    def update_feat_figure(jsonified_cleaned_data):

        if jsonified_cleaned_data is not None:
            dff = pd.read_json(jsonified_cleaned_data, orient='split')
            dff.sort_values("target_start_date", inplace=True)
            dff["feature_drift"] = np.where(pd.isna(dff["euclidean"]), dff["wasserstein"]/(dff["target_mean"]+1), dff["euclidean"]/(dff["target_dcount"]+1))
            dff["feature_name"] = np.where(pd.isna(dff["euclidean"]), dff["numeric_feature"], dff["categorical_feature"])
            fig = px.bar(dff, x="target_start_date", y="feature_drift", color="feature_name")    
        else:
            fig = go.Figure(data=[go.Histogram(x=[0] )])
        fig.update_layout(title_text="Data Drift by Feature", title_x=0.5)
        return fig



    app.run_server()