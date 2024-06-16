import json
import boto3


def lambda_handler(event, context):
    
    client = boto3.client("sqs")
    response = client.send_message(
        QueueUrl="https://sqs.us-east-2.amazonaws.com/525998537215/queue-guia-guide-generation-v1",
        MessageBody=json.dumps(event["body"])
    )
    
    data = { "requestId": response["ResponseMetadata"]["RequestId"] }
    print(f'Data to send queue:\n{data}')
    
    return {
        'statusCode': response["ResponseMetadata"]["HTTPStatusCode"],
        'body': json.dumps(data),
        'headers': {
            'Content-Type': 'application/json'
        }
    }