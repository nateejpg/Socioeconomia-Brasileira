import os
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../"))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET", "socioeconomia-brasil-gold")

DATA_DIR = os.path.join(ROOT_DIR, "data")

def get_s3_client():
    return boto3.client(
        's3', 
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

def clear_s3_prefix(s3_client, prefix):
    print(f"-> A executar Fresh Start (Limpeza) em: s3://{BUCKET_NAME}/{prefix}")
    objects_to_delete = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3_client.delete_objects(Bucket=BUCKET_NAME, Delete=delete_keys)

def upload_layer(layer_name):
    layer_path = os.path.join(DATA_DIR, layer_name, "ipca")
    s3_prefix = f"{layer_name}/ipca"
    
    if not os.path.exists(layer_path):
        return 0

    s3 = get_s3_client()
    clear_s3_prefix(s3, s3_prefix)

    uploaded_count = 0
    for root, dirs, files in os.walk(layer_path):
        for file in files:
            if file.endswith(".parquet") or file.endswith("_SUCCESS"):
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, layer_path)
                s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")

                try:
                    s3.upload_file(local_path, BUCKET_NAME, s3_path)
                    uploaded_count += 1
                except NoCredentialsError:
                    print("Credenciais AWS não encontradas")
                    return 0
    return uploaded_count

def run():
    print("A iniciar Upload: Inflação (IPCA)...")
    layers = ["bronze", "silver", "gold"]
    total = sum(upload_layer(layer) for layer in layers)
    print(f"-> Upload IPCA Concluído. {total} ficheiros reais enviados!")

if __name__ == "__main__":
    run()