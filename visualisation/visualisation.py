import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.express

from google.cloud import bigquery

import plotly.graph_objects
import plotly.figure_factory
import os
from google.cloud import bigquery_storage
import google.auth

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/20200191/Documents/data_engineering/DE2020/lab8/de2020-6-6a00f5d73faa.json'

#https://dash.plotly.com/
#https://plotly.com/

#https://cloud.google.com/bigquery/docs/bigquery-storage-python-pandas
# Read the data from Big Query table
def read_data():
    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    storage_client = bigquery_storage.BigQueryReadClient(credentials=credentials)

    gbq_client = bigquery.Client(credentials=credentials, project=project_id)
    sensor_table = bigquery.TableReference.from_string('de2020-6.machine.predictions')

    sensor_data = gbq_client.list_rows(
        sensor_table,
        selected_fields=[
            bigquery.SchemaField("timestamp", "INTEGER"),
            bigquery.SchemaField("RUL", "INTEGER"),
        ],
        max_results=1000000
    )

    df = sensor_data.to_dataframe(storage_client)
    df = df.sort_values(by=['timestamp'])
    return df.tail(35)



app = dash.Dash(__name__)

# The layout of the webapi
app.layout = html.Div([dcc.Graph(id='graph_RUL'),
                       dcc.Interval(id = 'realtime_graph',
                                    interval = 1000, #update every second (this is in miliseconds)
                                    n_intervals = 0)])

# Call back: to make the plot real-time
@app.callback(
    Output(component_id ='graph_RUL', component_property = 'figure'),
    [Input(component_id ='realtime_graph', component_property = 'n_intervals')]
)
def graph(n_inter):
    # Read last 35 rows of the table (i.e. the last 35 cycles)
    df = read_data()

    # Plot the time vs the predicted RUL
    fig1 = plotly.express.line(data_frame=df,
                               x= 'timestamp',
                               y='RUL',
                               title = 'Remaining Useful Lifetime of Engine 1',
                               template = 'plotly_dark',
                               range_y = [0, 400])
    return fig1

if __name__ == '__main__':
    app.run_server(debug=True)