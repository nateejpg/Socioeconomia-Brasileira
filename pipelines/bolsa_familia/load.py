import os
import boto3
from dotenv import load_dotenv
from boto3.s3.transfer import TransferConfig

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../"))
ENV_PATH = os.path.join(ROOT_DIR, ".env")
DATA_DIR = os.path.join(ROOT_DIR, "data")

load_dotenv(ENV_PATH)

def run():
    print("Iniciando Upload: Bolsa Família (Antigo)...")
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    # Define a pasta exata onde os parquets estao sendo salvos
    local_directory = os.path.join(DATA_DIR, "gold/fato_bolsa_familia")
    s3_prefix = "gold/fato_bolsa_familia/"

    if not os.path.exists(local_directory):
        print(f"Diretório não encontrado: {local_directory}")
        return

    config = TransferConfig(
        multipart_threshold=1024 * 25, 
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True
    )

    files_to_upload = []
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            if filename.endswith(".parquet"):
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                files_to_upload.append((local_path, s3_key))

    print(f"Enviando {len(files_to_upload)} arquivos para o S3...")
    
    for local, key in files_to_upload:
        try:
            s3_client.upload_file(local, bucket_name, key, Config=config)
        except Exception as e:
            print(f"Erro ao enviar {key}: {e}")

    print("Upload Bolsa Família concluído.")

if __name__ == "__main__":
    run()