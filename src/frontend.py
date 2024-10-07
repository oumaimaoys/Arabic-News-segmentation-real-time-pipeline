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

def create_wordcloud(text):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    
    # Save to a BytesIO object
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)  # Rewind the data
    return base64.b64encode(img.getvalue()).decode()


app = dash.Dash(__name__)

'''
bucket_name = "headlines1234bucket"
csv_file = f"s3a://{bucket_name}/predictions_hespress/predictions.csv" #change later

# Fetch data from S3 bucket
df = read_data_from_s3(bucket_name, key)
'''
df = pd.read_csv("/home/ouyassine/Documents/projects/data_engineering_1/data/fake_date.csv")

# Create a bar chart for topic distribution
fig = px.bar(df, x='topic', title="Topic Distribution")

#create a pie chart
fig_2 = px.pie(df, names='topic', title="Topic Distribution")

text = ' '.join(df['title'].astype(str) + ' ' + df['topic'].astype(str))



app.layout = html.Div([
    html.H1("News Headlines Topic Classification"),
    dcc.Graph(figure=fig),
    dcc.Graph(figure=fig_2),
    dcc.Graph(id='wordcloud-graph', config={'displayModeBar': False})

])

@app.callback(
    Output('wordcloud-graph', 'figure'),
    Input('wordcloud-graph', 'id')  # Just a trigger to generate on load
)
def update_wordcloud(_):
    img_data = create_wordcloud(text)
    return {
        'data': [],
        'layout': {
            'images': [{
                'source': f'data:image/png;base64,{img_data}',
                'xref': 'paper',
                'yref': 'paper',
                'x': 0,
                'y': 1,
                'sizex': 1,
                'sizey': 1,
                'xanchor': 'left',
                'yanchor': 'top',
                'opacity': 1,
                'layer': 'above'
            }],
            'xaxis': {'showgrid': False, 'zeroline': False, 'visible': False},
            'yaxis': {'showgrid': False, 'zeroline': False, 'visible': False},
            'height': 500,
            'width': 800,
        }
    }

if __name__ == '__main__':
    app.run_server(debug=True)
