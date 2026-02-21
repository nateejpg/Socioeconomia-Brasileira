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
    """Deleta todos os objetos dentro de um prefixo (pasta) especifico no S3."""
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            # Monta a lista de arquivos para deletar em lote
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )

def run():
    print("Iniciando Upload: IDH...")
    
    bucket_name = os.getenv('AWS_S3_BUCKET')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    if not bucket_name:
        raise ValueError(f"ERRO: A variável 'S3_BUCKET_NAME' não foi encontrada no arquivo {ENV_PATH}")
        
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

    layers = ["bronze/idh", "silver/idh", "gold/idh"]

    print("Executando Fresh Start (limpeza de arquivos antigos no S3)...")
    for layer in layers:
        s3_prefix = f"{layer}/"
        try:
            empty_s3_prefix(s3_client, bucket_name, s3_prefix)
            print(f"Limpeza concluída para o prefixo: s3://{bucket_name}/{s3_prefix}")
        except Exception as e:
            print(f"Aviso: Erro ao limpar {s3_prefix}: {e}")
    print("---------------------------------------------------------")
    
    # Upload dos novos arquivos
    for layer in layers:
        local_directory = os.path.join(DATA_DIR, layer)
        s3_prefix = f"{layer}/"

        if not os.path.exists(local_directory):
            print(f"Diretório não encontrado: {local_directory}")
            continue

        files_to_upload = []
        for root, dirs, files in os.walk(local_directory):
            for filename in files:
                if filename.endswith(".parquet"):
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_path, local_directory)
                    s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                    files_to_upload.append((local_path, s3_key))

        print(f"Enviando {len(files_to_upload)} arquivos da camada {layer} para o S3...")
        
        for local, key in files_to_upload:
            try:
                s3_client.upload_file(local, bucket_name, key, Config=config)
            except Exception as e:
                print(f"Erro ao enviar {key}: {e}")

    print("Upload IDH concluído.")

if __name__ == "__main__":
    run()