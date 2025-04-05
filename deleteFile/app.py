#!/usr/bin/env python3
import json
import os
import urllib.parse
import psycopg2

CONN_STRING = os.getenv("CONN_STRING")
db = psycopg2.connect(CONN_STRING)

def lambda_handler(event, context):
    key = event["key"]
    removed_file_size = event["totalBytes"]
    
    try:
        org = key.split("/")[0]
        cursor = db.cursor()
        query = """
                INSERT INTO resource_usage (organization_id, file_count, total_bytes)
                VALUES (%s, %s, %s)
                ON CONFLICT (organization_id)
                DO UPDATE SET
                    file_count = resource_usage.file_count - %s,
                    total_bytes = resource_usage.total_bytes - %s;
                """

        data = (org,1,removed_file_size,1,removed_file_size)

        # Update the storage data 
        cursor.execute(query,data)
        db.commit()
        cursor.close()

        return {        
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Storage data updated successfully'
            })
        }

    except Exception as e:
        print(f"Error processing S3 remove: {e}")
        raise e



