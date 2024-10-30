import boto3
import pandas as pd
from io import StringIO

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import base64
import io

def read_data_from_s3(bucket_name, key):
    s3_client = boto3.client('s3')
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = csv_obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(body))
    return df

app = dash.Dash(__name__)

'''
bucket_name = "headlines1234bucket"
csv_file = f"s3a://{bucket_name}/hespress_pred/predictions.csv" #change later

# Fetch data from S3 bucket
df = read_data_from_s3(bucket_name, key)
'''
df = pd.read_csv("/home/ouyassine/Documents/projects/data_engineering_1/data/fake_date.csv")



#create a pie chart
fig_2 = px.pie(df, names='topic', title="Topic Distribution")

text = ' '.join(df['title'].astype(str) + ' ' + df['topic'].astype(str))



app.layout = html.Div([
    html.H1("News Headlines Topic Classification", style={'textAlign': 'center'}  ),
    dcc.Graph(figure=fig_2),

    html.H1("Article Filter"),
    
    dcc.Dropdown(
        id='topic-dropdown',
        options=[
            {'label': 'Sport', 'value': 'sport'},
            {'label': 'Divers', 'value': 'divers'},
            {'label': 'Politique', 'value': 'politique'},
            {'label': 'Economie', 'value': 'economie'}
        ],
        value='sport',  # Default value
        clearable=False
    ),
    
    html.Div(id='articles-list')
    
])

# Callback to update the articles list based on selected topic
@app.callback(
    Output('articles-list', 'children'),
    Input('topic-dropdown', 'value')
)
def update_articles(selected_topic):
    # Filter articles based on selected topic
    filtered_articles = df[df['topic'] == selected_topic]
    
    # Create a list of article titles
    article_titles = filtered_articles['title'].tolist()
    
    # Return the article titles as a list of HTML elements
    return html.Ul([html.Li(title) for title in article_titles])




if __name__ == '__main__':
    app.run_server(debug=True)
