import os
import boto3
from dotenv import load_dotenv
from boto3.s3.transfer import TransferConfig

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../"))
ENV_PATH = os.path.join(ROOT_DIR, ".env")
DATA_DIR = os.path.join(ROOT_DIR, "data")

load_dotenv(ENV_PATH)

def empty_s3_prefix(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})

def run():
    bucket_name = os.getenv('AWS_S3_BUCKET')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME nao encontrado.")
        
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    config = TransferConfig(
        multipart_threshold=1024 * 25, 
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True
    )

    layer = "gold/populacao"
    s3_prefix = f"{layer}/"
    
    empty_s3_prefix(s3_client, bucket_name, s3_prefix)
    
    local_directory = os.path.join(DATA_DIR, layer)

    if os.path.exists(local_directory):
        for root, dirs, files in os.walk(local_directory):
            for filename in files:
                if filename.endswith(".parquet") or filename.endswith(".csv"):
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_path, local_directory)
                    s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                    s3_client.upload_file(local_path, bucket_name, s3_key, Config=config)

if __name__ == "__main__":
    run()