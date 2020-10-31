import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.express
import pandas as pd
from google.cloud import bigquery
import datetime as dt

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/20200191/Documents/data_engineering/DE2020/lab8/de2020-6-6a00f5d73faa.json'

#https://dash.plotly.com/
#https://plotly.com/

def read_data():
    n_rows = 100
    client = bigquery.Client()

    query = """
        SELECT 
            * 
        FROM `de2020-6.sensordata.RUL_predictions`
        order by Eventtime
        LIMIT {}
        """

    print('step 1')
    bq_table = client.query(query.format(n_rows))
    return {'Eventtime': [dt.datetime.fromtimestamp(row[0]) for row in bq_table],
            'RUL': [row[1] for row in bq_table]}

def graph():
    dict_data = read_data()
    df = pd.DataFrame(dict_data)
    print(df)
    figure = plotly.express.line(data_frame = df,
                                 x= 'Eventtime',
                                 y = 'RUL',
                                 title = 'Remaining Useful Lifetime of Engine 1')
    app.layout = html.Div([dcc.Graph(id='graph_RUL', figure = figure)])

app = dash.Dash(__name__)

#@app.callback(
#    dash.dependencies.Output(component_id ='graph_RUL', component_property = 'plot')
#)
#def graph():
#    dict_data = read_data()
#    df = pd.DataFrame(dict_data)
#    print(df)
#    figure = plotly.express.line(data_frame = df, x= 'Eventtime', y = 'RUL')
#    app.layout = html.Div([dcc.Graph(id='graph_RUL', figure = figure)])

#return figure

if __name__ == '__main__':
    app.run_server()