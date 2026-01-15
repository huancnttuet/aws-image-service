import json
import boto3
import base64
import uuid
from datetime import datetime
import os
import re

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['TABLE_NAME']

def parse_multipart(body_bytes, content_type):
    """Simple multipart parser that preserves binary data"""
    # Extract boundary
    match = re.search(r'boundary=(.+?)(?:;|$)', content_type)
    if not match:
        return None, None, None
    
    boundary = match.group(1).strip('"').strip()
    boundary_bytes = b'--' + boundary.encode('utf-8')
    
    # Split parts
    parts = body_bytes.split(boundary_bytes)
    
    for part in parts:
        if len(part) < 10:
            continue
            
        # Find headers end
        header_end = part.find(b'\r\n\r\n')
        if header_end == -1:
            header_end = part.find(b'\n\n')
            if header_end == -1:
                continue
            header_sep = b'\n\n'
        else:
            header_sep = b'\r\n\r\n'
        
        headers = part[:header_end].decode('utf-8', errors='replace')
        file_content = part[header_end + len(header_sep):]
        
        # Check if it's a file
        if 'filename=' not in headers:
            continue
        
        # Extract filename
        fn_match = re.search(r'filename="([^"]+)"', headers)
        if not fn_match:
            fn_match = re.search(r'filename=([^\s;]+)', headers)
        filename = fn_match.group(1) if fn_match else 'unknown'
        
        # Extract content type
        ct_match = re.search(r'Content-Type:\s*([^\r\n]+)', headers, re.IGNORECASE)
        file_content_type = ct_match.group(1).strip() if ct_match else 'application/octet-stream'
        
        # Clean trailing boundary/CRLF
        if file_content.endswith(b'\r\n'):
            file_content = file_content[:-2]
        if file_content.endswith(b'\n'):
            file_content = file_content[:-1]
        if file_content.endswith(b'--'):
            file_content = file_content[:-2]
        if file_content.endswith(b'\r\n'):
            file_content = file_content[:-2]
        
        return file_content, filename, file_content_type
    
    return None, None, None


def lambda_handler(event, context):
    """
    POST /upload
    Upload file to S3 and store metadata in DynamoDB
    """
    try:
        content_type = event.get('headers', {}).get('content-type', '') or \
                       event.get('headers', {}).get('Content-Type', '')
        
        if 'multipart/form-data' not in content_type:
            return response(400, {'error': 'Content-Type must be multipart/form-data'})
        
        # Get body bytes
        body = event.get('body', '')
        is_base64 = event.get('isBase64Encoded', False)
        
        if is_base64:
            body_bytes = base64.b64decode(body)
        else:
            # Already bytes or string that needs encoding
            body_bytes = body.encode('utf-8') if isinstance(body, str) else body
        
        # Parse multipart
        file_data, filename, file_content_type = parse_multipart(body_bytes, content_type)
        
        if not file_data or not filename:
            return response(400, {'error': 'No file found in request'})
        
        # Generate ID and key
        image_id = str(uuid.uuid4())
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else 'bin'
        s3_key = f"images/{image_id}.{ext}"
        
        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=file_data,
            ContentType=file_content_type
        )
        
        # Save metadata to DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        timestamp = datetime.utcnow().isoformat()
        
        table.put_item(Item={
            'imageId': image_id,
            'filename': filename,
            'contentType': file_content_type,
            's3Key': s3_key,
            'size': len(file_data),
            'uploadedAt': timestamp,
            'bucket': BUCKET_NAME
        })
        
        # Generate download URL
        download_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': s3_key},
            ExpiresIn=3600
        )
        
        return response(201, {
            'message': 'Upload successful',
            'imageId': image_id,
            'filename': filename,
            's3Key': s3_key,
            'downloadUrl': download_url,
            'size': len(file_data),
            'contentType': file_content_type
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return response(500, {'error': 'Internal server error', 'message': str(e)})


def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body)
    }
