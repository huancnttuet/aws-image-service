import json
import boto3
import os
from decimal import Decimal

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['TABLE_NAME']

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    """
    GET /images/{id}
    Get specific image metadata and generate presigned URL
    """
    try:
        # Get image ID from path parameters
        image_id = event.get('pathParameters', {}).get('id')
        
        if not image_id:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Missing image ID'
                })
            }
        
        # Get metadata from DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        response = table.get_item(
            Key={'imageId': image_id}
        )
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Image not found'
                })
            }
        
        item = response['Item']
        
        # Generate presigned URL (valid for 1 hour)
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': item['s3Key']
            },
            ExpiresIn=3600  # 1 hour
        )
        
        # Add presigned URL to response
        item['downloadUrl'] = presigned_url
        
        print(f"Retrieved image: {image_id}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'image': item
            }, cls=DecimalEncoder)
        }
        
    except Exception as e:
        print(f"Error getting image: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }
