import json
import boto3
import base64
import uuid
from datetime import datetime
import os

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['TABLE_NAME']

def lambda_handler(event, context):
    """
    POST /upload
    Upload image to S3 and store metadata in DynamoDB
    
    Expected body:
    {
        "filename": "photo.jpg",
        "content": "base64_encoded_image_data",
        "contentType": "image/jpeg"
    }
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        
        # Validate required fields
        if not all(k in body for k in ['filename', 'content', 'contentType']):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Missing required fields: filename, content, contentType'
                })
            }
        
        # Generate unique ID
        image_id = str(uuid.uuid4())
        
        # Extract file extension
        filename = body['filename']
        file_extension = filename.split('.')[-1] if '.' in filename else 'jpg'
        s3_key = f"images/{image_id}.{file_extension}"
        
        # Decode base64 content
        image_data = base64.b64decode(body['content'])
        
        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=image_data,
            ContentType=body['contentType'],
            Metadata={
                'original-filename': filename,
                'image-id': image_id
            }
        )
        
        # Store metadata in DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        timestamp = datetime.utcnow().isoformat()
        
        table.put_item(
            Item={
                'imageId': image_id,
                'filename': filename,
                'contentType': body['contentType'],
                's3Key': s3_key,
                'size': len(image_data),
                'uploadedAt': timestamp,
                'bucket': BUCKET_NAME
            }
        )
        
        print(f"Successfully uploaded image: {image_id}")
        
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Image uploaded successfully',
                'imageId': image_id,
                'filename': filename,
                's3Key': s3_key,
                'uploadedAt': timestamp
            })
        }
        
    except Exception as e:
        print(f"Error uploading image: {str(e)}")
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
