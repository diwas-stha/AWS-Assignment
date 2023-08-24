import json
import os
import boto3
import urllib3
import pandas as pd
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


# Get API url from environment variables
API_URL = os.environ.get("API_URL_ANIME")

# Get API headers from environment variables
API_KEY = os.environ.get('ANIME_API_KEY')
API_HOST = os.environ.get('ANIME_API_HOST')

url = API_URL
querystring = {"page": "5", "size": "300"}
headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": API_HOST
}

http = urllib3.PoolManager()


def lambda_handler(event, context):
    # TODO implement

    logger.info(msg="*******Lambda initialized******")

    # Make the API request
    response = http.request("GET", url, headers=headers, fields=querystring)

    # Put Raw data
    s3.put_object(
        Body=response.data,
        Bucket='apprentice-training-diwas-raw-ml-dev',
        Key='anime_raw.json'
    )

    logger.info(msg="******Dumped raw anime data **********")

    # Transformations

    # Parse the JSON response
    response_data = response.data.decode("utf-8")
    anime_data = json.loads(response_data)['data']

    # Convert the list of dictionaries to DataFrame

    anime_df = pd.DataFrame(anime_data)

    # print(anime_df.head())

    # Drop rows with any missing values
    anime_df.dropna(inplace=True)

    # Fill missing values with a specific value
    anime_df['episodes'].fillna(0, inplace=True)

    # Remove duplicate data
    anime_df.drop_duplicates(subset=['title'], keep='first', inplace=True)

    # Dropping columns
    columns_to_drop = ['hasEpisode', 'hasRanking', 'image', 'link', 'thumb']
    anime_df.drop(columns=columns_to_drop, inplace=True)

    # Filter anime with type 'TV'
    tv_anime_df = anime_df[anime_df['type'] == 'TV'].copy()

    # Shorten the synopsis to a certain number of characters
    max_synopsis_length = 100  # You can adjust this value
    tv_anime_df['short_synopsis'] = tv_anime_df['synopsis'].str[:max_synopsis_length]

    # print(tv_anime_df.head())

    # Put cleaned/transformed data
    tv_anime_df_json = tv_anime_df.to_json(orient="records")
    tv_anime_df_bytes = tv_anime_df_json.encode("utf-8")

    s3.put_object(
        Body=tv_anime_df_bytes,
        Bucket='apprentice-training-diwas-cleaned-ml-dev',
        Key='tv_anime_data.json'
    )

    logger.info(msg="*****Dumped cleaned data anime data*******")

    # Insert data into AWS RDS
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )

        cur = conn.cursor()

        for index, row in tv_anime_df.iterrows():
            cur.execute(
                '''
                INSERT INTO etl_training_diwas_anime_table (
                    _id, title, alternativeTitles, ranking, genres, episodes, status, synopsis, type, short_synopsis
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ''',
                (
                    row['_id'],
                    row['title'],
                    row['alternativeTitles'],
                    row['ranking'],
                    row['genres'],
                    row['episodes'],
                    row['status'],
                    row['synopsis'],
                    row['type'],
                    row['short_synopsis']
                )
            )

        conn.commit()

    except Exception as e:
        print("Error:", e)
        conn.rollback()

    finally:
        if conn:
            conn.close()

    logger.info(msg="*******Inserted into table in AWS RDS******")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
