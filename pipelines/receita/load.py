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
BUCKET_NAME = os.getenv("AWS_S3_BUCKET")

DATA_DIR = os.path.join(ROOT_DIR, "data")

def upload_folder_to_s3(local_folder, s3_folder):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGION)

    if not os.path.exists(local_folder):
        print(f"Pasta nao encontrada: {local_folder}")
        return

    print(f"Iniciando upload de: {local_folder} -> s3://{BUCKET_NAME}/{s3_folder}")

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            
            relative_path = os.path.relpath(local_path, local_folder)
            s3_path = os.path.join(s3_folder, relative_path).replace("\\", "/")

            try:
                s3.upload_file(local_path, BUCKET_NAME, s3_path)
            except FileNotFoundError:
                print(f"Arquivo nao encontrado: {local_path}")
            except NoCredentialsError:
                print("Credenciais AWS nao encontradas")
                return

def run_upload():

    upload_folder_to_s3(os.path.join(DATA_DIR, "bronze/receita"), "bronze/receita")
    upload_folder_to_s3(os.path.join(DATA_DIR, "silver/receita"), "silver/receita")
    upload_folder_to_s3(os.path.join(DATA_DIR, "gold/receita"), "gold/receita")
    
    print("Upload Receita Concluido!")

if __name__ == "__main__":
    run_upload()