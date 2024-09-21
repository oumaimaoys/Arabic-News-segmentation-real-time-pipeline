import boto3
import pandas as pd
from io import StringIO

def read_data_from_s3(bucket_name, key):
    s3_client = boto3.client('s3')
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = csv_obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(body))
    return df

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

app = dash.Dash(__name__)

# Fetch data from S3 bucket
df = read_data_from_s3('your-bucket-name', 'predictions/predictions.csv')

# Create a bar chart for topic distribution
fig = px.bar(df, x='topic', y='count', title="Topic Distribution")

app.layout = html.Div([
    html.H1("News Headlines Topic Classification"),
    dcc.Graph(figure=fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)
