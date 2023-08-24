import json
import os
import boto3
import urllib3
import pandas as pd
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_URL = os.environ.get("API_URL")

s3 = boto3.client("s3")

url = API_URL
http = urllib3.PoolManager()


def lambda_handler(event, context):
    # TODO implement

    logger.info(msg="*******Lambda initialized******")
    response = http.request('GET', url)

    # Put Raw data
    s3.put_object(
        Body=response.data,
        Bucket='apprentice-training-diwas-raw-ml-dev',
        Key='got_raw.json'
    )

    logger.info(msg="*************Dumped raw data*")

    # Transformations

    data = json.loads(response.data.decode('utf-8'))
    df = pd.DataFrame(data)

    # print(df.head())

    # Dropping columns
    columns_to_drop = ["titles", "seats", "heir", "founded",
                       "founder", "diedOut", "cadetBranches", "ancestralWeapons"]
    df = df.drop(columns=columns_to_drop)

    # Filter data to include only houses who have words
    filtered_df = df[df["words"] != ""]

    #  Put cleaned/transformed data
    filtered_df_json = filtered_df.to_json(orient="records")
    filtered_df_bytes = filtered_df_json.encode("utf-8")

    s3.put_object(
        Body=filtered_df_bytes,
        Bucket='apprentice-training-diwas-cleaned-ml-dev',
        Key='got_cleaned.json'
    )

    logger.info(msg="Dumped cleaned data")

    # Insert data into AWS RDS
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )

        cur = conn.cursor()

        for index, row in filtered_df.iterrows():
            cur.execute(
                '''INSERT INTO etl_training_diwas_got_houses_table (
                        url, name, region, coatOfArms, words, currentLord, overlord, swornMembers
                    ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (
                    row['url'],
                    row['name'],
                    row['region'],
                    row['coatOfArms'],
                    row['words'],
                    row['currentLord'],
                    row['overlord'],
                    row['swornMembers']
                )
            )

        conn.commit()

    except Exception as e:
        print("Error:", e)
        conn.rollback()

    finally:
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
