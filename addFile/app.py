#!/usr/bin/env python3
import json
import os
import urllib.parse
import psycopg2

CONN_STRING = os.getenv("CONN_STRING")
db = psycopg2.connect(CONN_STRING)


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        org_id = key.split("/")[0]
        file_id = f"file_{key.split("/")[2].split('_')[1]}"
        cursor = db.cursor()
        query = """
                INSERT INTO resource_usage (organization_id, file_count, total_bytes)
                VALUES (%s, %s, %s)
                ON CONFLICT (organization_id)
                DO UPDATE SET
                    file_count = resource_usage.file_count + %s,
                    total_bytes = resource_usage.total_bytes + %s;
                """
 
        # Get the size of the added file
        new_file_size = event['Records'][0]['s3']['object']['size']

        data = (org_id,1,new_file_size,1,new_file_size)

        # Update the storage data 
        cursor.execute(query,data)

        file_size_query = """
            UPDATE files SET file_size = %s WHERE file_id = %s
        """
        
        cursor.execute(file_size_query, (new_file_size,file_id))

        db.commit()
        cursor.close()

        return {        
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Storage data updated successfully'
            })
        }

    except Exception as e:
        print(f"Error processing S3 add: {e}")
        raise e



