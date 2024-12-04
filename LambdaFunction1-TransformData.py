import json
import boto3
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    print("TriggerSparkJob Lambda function has been triggered")
    print(f"Event data: {event}")
    # Get the bucket and object name from the event that triggered the Lambda
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']

    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Download the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read()
    # Try UTF-8 first, fallback to other encodings if necessary
    try:
        file_content = content.decode('utf-8')
    except UnicodeDecodeError:
        file_content = content.decode('ISO-8859-1')  # Try a fallback encoding

    # Load CSV into pandas DataFrame
    df = pd.read_csv(StringIO(file_content), delimiter=",", na_values=["", "NA", "null"])
    print(df.info())

    # Transform data (example: convert 'credit_limit' to float)
    if 'credit_limit' not in df.columns:
        df['credit_limit'] = 0  # or any other default value
    else:
        df['credit_limit'] = df['credit_limit'].replace({r'\$': ''}, regex=True).astype(float)

    # Initialize SQS client
    sqs = boto3.client('sqs')
    
    # Send transformed data to SQS
    transformed_data = df.to_dict(orient='records')
    queue_url = 'https://sqs.eu-north-1.amazonaws.com/340752819684/TransformedDataQueue'
    
    for record in transformed_data:
        # Send each record to the queue
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(record)
        )

    return {
        'statusCode': 200,
        'body': json.dumps(f"Successfully processed file {file_name} and sent to SQS")
    }