import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('R2_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('R2_SECRET_KEY'),
    region_name='auto'
)

print("Listing buckets:")
try:
    resp = s3.list_buckets()
    for b in resp['Buckets']:
        print(f" - {b['Name']}")
except Exception as e:
    print(e)
