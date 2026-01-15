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
        # Log event for debugging
        print(f"Event: {json.dumps(event)}")
        
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
        
        print(f"Is base64: {is_base64}, Body length: {len(body)}")
        
        if is_base64:
            body_bytes = base64.b64decode(body)
        else:
            body_bytes = body.encode('utf-8') if isinstance(body, str) else body
        
        print(f"Body bytes length: {len(body_bytes)}")
        
        # Parse multipart form data manually
        import re
        
        # Extract boundary from content-type
        boundary_match = re.search(r'boundary=([^;]+)', content_type)
        if not boundary_match:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No boundary found in Content-Type'
                })
            }
        
        boundary = boundary_match.group(1).strip('"')
        boundary_bytes = ('--' + boundary).encode()
        
        print(f"Boundary: {boundary}")
        
        # Split by boundary
        parts = body_bytes.split(boundary_bytes)
        
        file_data = None
        filename = None
        content_type_file = None
        
        for part in parts:
            if not part or part == b'--\r\n' or part == b'--':
                continue
            
            # Split headers and body
            if b'\r\n\r\n' in part:
                headers_section, body_section = part.split(b'\r\n\r\n', 1)
            elif b'\n\n' in part:
                headers_section, body_section = part.split(b'\n\n', 1)
            else:
                continue
            
            headers_str = headers_section.decode('utf-8', errors='ignore')
            
            # Check if this part contains a file
            if 'filename=' in headers_str:
                # Extract filename
                filename_match = re.search(r'filename="([^"]+)"', headers_str)
                if filename_match:
                    filename = filename_match.group(1)
                
                # Extract content type
                content_type_match = re.search(r'Content-Type:\s*([^\r\n]+)', headers_str, re.IGNORECASE)
                if content_type_match:
                    content_type_file = content_type_match.group(1).strip()
                else:
                    # Guess content type from filename
                    ext = filename.split('.')[-1].lower() if '.' in filename else ''
                    content_type_map = {
                        'jpg': 'image/jpeg',
                        'jpeg': 'image/jpeg',
                        'png': 'image/png',
                        'gif': 'image/gif',
                        'webp': 'image/webp',
                        'bmp': 'image/bmp',
                        'svg': 'image/svg+xml'
                    }
                    content_type_file = content_type_map.get(ext, 'application/octet-stream')
                
                # Remove trailing \r\n or \n
                file_data = body_section.rstrip(b'\r\n')
                
                print(f"Found file: {filename}, Content-Type: {content_type_file}, Size: {len(file_data)} bytes")
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
        
        print(f"Uploaded to S3: {s3_key}, Size: {len(file_data)} bytes")
        
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
                'uploadedAt': timestamp,
                'size': len(file_data),
                'contentType': content_type_file
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
