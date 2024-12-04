import json
from pymongo import MongoClient

def lambda_handler(event, context):
    # Initialize MongoDB client
    client = MongoClient("mongodb+srv://priyanka:Od9Lkvr0S4rBQzO1@cluster0.ssdor.mongodb.net/")
    db = client.test1  
    collection = db.cards_data1 

    for record in event['Records']:
        # The body of the SQS message (which is a transformed record)
        message_body = json.loads(record['body'])
        
        # Insert data into MongoDB
        collection.insert_one(message_body)

    return {
        'statusCode': 200,
        'body': json.dumps(f"Successfully inserted data into MongoDB")
    }