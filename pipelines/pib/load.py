import os
import boto3
from botocore.exceptions import NoCredentialsError

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))
BUCKET_NAME = "socioeconomia-brasil-gold"

def upload_layer(layer_name):
    layer_path = os.path.join(DATA_DIR, layer_name, "pib")
    s3_prefix = f"{layer_name}/pib"
    
    if not os.path.exists(layer_path):
        print(f"Diretório não encontrado: {layer_path}")
        return 0

    s3 = boto3.client('s3')
    print(f"Iniciando upload da camada {layer_name.upper()} para s3://{BUCKET_NAME}/{s3_prefix}")

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
                except Exception as e:
                    print(f"Erro ao enviar {file}: {e}")
    
    print(f"Camada {layer_name} finalizada. Arquivos: {uploaded_count}")
    return uploaded_count

def run():
    total_files = 0
    layers = ["bronze", "silver", "gold"]
    
    try:
        for layer in layers:
            total_files += upload_layer(layer)
        
        print(f"\nUpload total concluído. Total de arquivos no S3: {total_files}")
    except NoCredentialsError:
        print("Erro: Credenciais AWS não encontradas.")

if __name__ == "__main__":
    run()