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
        
        print(f"Is base64: {is_base64}, Body length: {len(body)}")
        
        if is_base64:
            body_bytes = base64.b64decode(body)
        else:
            body_bytes = body.encode('latin-1') if isinstance(body, str) else body
        
        print(f"Body bytes length: {len(body_bytes)}")
        
        # Parse multipart form data
        import re
        import cgi
        from io import BytesIO
        
        # Extract boundary from content-type
        boundary_match = re.search(r'boundary=([^;\s]+)', content_type)
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
        
        print(f"Boundary: {boundary}")
        
        # Use cgi.FieldStorage to parse multipart data
        environ = {
            'REQUEST_METHOD': 'POST',
            'CONTENT_TYPE': content_type,
            'CONTENT_LENGTH': str(len(body_bytes))
        }
        
        fp = BytesIO(body_bytes)
        form = cgi.FieldStorage(fp=fp, environ=environ, keep_blank_values=True)
        
        file_data = None
        filename = None
        content_type_file = None
        
        # Try to find file field
        for key in form.keys():
            field = form[key]
            if hasattr(field, 'filename') and field.filename:
                filename = field.filename
                file_data = field.file.read()
                content_type_file = field.type if hasattr(field, 'type') and field.type else None
                print(f"Found file via cgi: {filename}, type: {content_type_file}")
                break
        
        # If cgi failed, try manual parsing
        if not file_data or not filename:
            print("cgi parsing failed, trying manual parsing...")
            
            boundary_bytes = ('--' + boundary).encode('latin-1')
            end_boundary = boundary_bytes + b'--'
            
            # Split by boundary
            parts = body_bytes.split(boundary_bytes)
            
            for part in parts:
                # Skip empty parts and end marker
                if not part or part.strip() in [b'', b'--', b'--\r\n']:
                    continue
                
                # Remove leading CRLF
                if part.startswith(b'\r\n'):
                    part = part[2:]
                elif part.startswith(b'\n'):
                    part = part[1:]
                
                # Find header/body separator
                separator = None
                if b'\r\n\r\n' in part:
                    separator = b'\r\n\r\n'
                elif b'\n\n' in part:
                    separator = b'\n\n'
                else:
                    continue
                
                header_part, body_part = part.split(separator, 1)
                headers_str = header_part.decode('utf-8', errors='ignore')
                
                # Check for filename in Content-Disposition
                if 'filename=' not in headers_str:
                    continue
                
                # Extract filename
                filename_match = re.search(r'filename="([^"]*)"', headers_str)
                if not filename_match:
                    filename_match = re.search(r"filename=([^\s;]+)", headers_str)
                
                if filename_match:
                    filename = filename_match.group(1)
                
                # Extract content type
                ct_match = re.search(r'Content-Type:\s*([^\r\n]+)', headers_str, re.IGNORECASE)
                if ct_match:
                    content_type_file = ct_match.group(1).strip()
                
                # Remove trailing boundary marker and CRLF
                # The body ends before the next boundary or end of data
                if body_part.endswith(b'--\r\n'):
                    body_part = body_part[:-4]
                elif body_part.endswith(b'--'):
                    body_part = body_part[:-2]
                
                # Remove trailing CRLF (part delimiter)
                if body_part.endswith(b'\r\n'):
                    body_part = body_part[:-2]
                elif body_part.endswith(b'\n'):
                    body_part = body_part[:-1]
                
                file_data = body_part
                print(f"Found file via manual: {filename}, size: {len(file_data)}")
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
        
        # Determine content type if not set
        if not content_type_file:
            ext = filename.split('.')[-1].lower() if '.' in filename else ''
            content_type_map = {
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif',
                'webp': 'image/webp',
                'bmp': 'image/bmp',
                'svg': 'image/svg+xml',
                'pdf': 'application/pdf'
            }
            content_type_file = content_type_map.get(ext, 'application/octet-stream')
        
        # Generate unique ID
        image_id = str(uuid.uuid4())
        
        # Extract file extension
        file_extension = filename.split('.')[-1].lower() if '.' in filename else 'bin'
        s3_key = f"images/{image_id}.{file_extension}"
        
        print(f"Uploading to S3: {s3_key}, Size: {len(file_data)} bytes, ContentType: {content_type_file}")
        
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
