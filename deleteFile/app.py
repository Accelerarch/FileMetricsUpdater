#!/usr/bin/env python3
import json
import urllib.parse
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        storage_usage_key = key.split("/")[0]+"/usage.json"
        # Get storage usage file
        try:
            response = s3.get_object(Bucket=bucket, Key=storage_usage_key)
            storage_data = json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            raise e

        # Get the previous storage usage metrics
        file_count = storage_data.get('fileCount', 0)
        total_bytes = storage_data.get('totalBytes', 0)
 
        # Get the size of the added file
        new_file_size = event['Records'][0]['s3']['object']['size']
        file_count = max(0,file_count-1)
        total_bytes = max(0,new_file_size)

        # Update the storage data JSON
        storage_data['fileCount'] = file_count
        storage_data['totalBytes'] = total_bytes
        s3.put_object(
            Bucket=bucket,
            Key=storage_usage_key,  
            Body=json.dumps(storage_data),
            ContentType='application/json'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'fileCount': file_count,
                'totalBytes': total_bytes,
                'message': 'Storage data updated successfully'
            })
        }

    except Exception as e:
        print(f"Error processing S3 add: {e}")
        raise e



