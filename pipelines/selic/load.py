import os
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# Configurações de caminhos e env
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../"))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

# Credenciais do seu .env
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET", "socioeconomia-brasil-gold") # Confirme se o nome está certo

DATA_DIR = os.path.join(ROOT_DIR, "data")

def get_s3_client():
    """Cria o cliente S3 com as suas credenciais explícitas."""
    return boto3.client(
        's3', 
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

def clear_s3_prefix(s3_client, prefix):
    print(f"-> Executando Fresh Start (Limpeza) em: s3://{BUCKET_NAME}/{prefix}")
    objects_to_delete = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3_client.delete_objects(Bucket=BUCKET_NAME, Delete=delete_keys)

def upload_layer(layer_name):
    layer_path = os.path.join(DATA_DIR, layer_name, "selic")
    s3_prefix = f"{layer_name}/selic"
    
    if not os.path.exists(layer_path):
        print(f"Pasta nao encontrada: {layer_path}")
        return 0

    s3 = get_s3_client()
    
    clear_s3_prefix(s3, s3_prefix)

    print(f"Iniciando upload de: {layer_path} -> s3://{BUCKET_NAME}/{s3_prefix}")
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
                    print("Credenciais AWS nao encontradas")
                    return 0
                except Exception as e:
                    print(f"Erro ao enviar {file}: {e}")
                    
    return uploaded_count

def run():
    print("Iniciando Upload: Taxa Selic...")
    layers = ["bronze", "silver", "gold"]
    total = sum(upload_layer(layer) for layer in layers)
    print(f"-> Upload Selic Concluído. {total} arquivos reais enviados!")

if __name__ == "__main__":
    run()