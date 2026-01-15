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
    
    Expected: multipart/form-data with 'file' field
    """
    try:
        # Parse multipart/form-data
        content_type = event.get('headers', {}).get('content-type', '') or event.get('headers', {}).get('Content-Type', '')
        
        if 'multipart/form-data' not in content_type:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Content-Type must be multipart/form-data'
                })
            }
        
        # Get the body (base64 encoded in API Gateway)
        body = event.get('body', '')
        is_base64 = event.get('isBase64Encoded', False)
        
        if is_base64:
            body = base64.b64decode(body)
        else:
            body = body.encode('utf-8')
        
        # Parse multipart form data
        import email
        from email import policy
        from io import BytesIO
        
        # Create email message from body
        msg = email.message_from_bytes(
            b'Content-Type: ' + content_type.encode() + b'\r\n\r\n' + body,
            policy=policy.default
        )
        
        # Extract file from form data
        file_data = None
        filename = None
        content_type_file = None
        
        for part in msg.walk():
            content_disposition = part.get('Content-Disposition', '')
            if 'filename=' in content_disposition:
                # Extract filename
                import re
                match = re.search(r'filename="?([^"]+)"?', content_disposition)
                if match:
                    filename = match.group(1)
                
                # Get file data
                file_data = part.get_payload(decode=True)
                content_type_file = part.get_content_type() or 'application/octet-stream'
                break
        
        if not file_data or not filename:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No file uploaded. Please use form field name "file"'
                })
            }
        
        # Generate unique ID
        image_id = str(uuid.uuid4())
        
        # Extract file extension
        file_extension = filename.split('.')[-1] if '.' in filename else 'jpg'
        s3_key = f"images/{image_id}.{file_extension}"
        
        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=file_data,
            ContentType=content_type_file,
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
                'contentType': content_type_file,
                's3Key': s3_key,
                'size': len(file_data),
                'uploadedAt': timestamp,
                'bucket': BUCKET_NAME
            }
        )
        
        # Generate presigned URL for download
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': s3_key
            },
            ExpiresIn=3600  # 1 hour
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
                'downloadUrl': presigned_url,
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
