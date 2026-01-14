import json
import boto3
import os
from decimal import Decimal

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['TABLE_NAME']

# Helper to convert Decimal to int/float for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    """
    GET /images
    List all uploaded images from DynamoDB
    
    Optional query parameters:
    - limit: Number of items to return (default: 50)
    """
    try:
        # Get query parameters
        params = event.get('queryStringParameters') or {}
        limit = int(params.get('limit', 50))
        
        # Query DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        
        response = table.scan(
            Limit=limit
        )
        
        items = response.get('Items', [])
        
        print(f"Retrieved {len(items)} images from DynamoDB")

        # Generate presigned URLs for each image
        for item in items:
            if 's3Key' in item:
                try:
                    presigned_url = s3.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': BUCKET_NAME,
                            'Key': item['s3Key']
                        },
                        ExpiresIn=3600  # 1 hour
                    )
                    item['downloadUrl'] = presigned_url
                except Exception as url_error:
                    print(f"Error generating presigned URL for {item.get('imageId')}: {str(url_error)}")
                    item['downloadUrl'] = None
        
        print(f"Retrieved {len(items)} images from DynamoDB with presigned URLs")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'count': len(items),
                'images': items
            }, cls=DecimalEncoder)
        }
        
    except Exception as e:
        print(f"Error listing images: {str(e)}")
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
