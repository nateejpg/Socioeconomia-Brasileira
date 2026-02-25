import os
import boto3
from botocore.exceptions import NoCredentialsError

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))
BUCKET_NAME = "socioeconomia-brasil-gold"

def clear_s3_prefix(s3_client, bucket, prefix):
    """Deleta todos os objetos em um prefixo específico para evitar duplicidade."""
    print(f"Limpando pasta antiga em s3://{bucket}/{prefix}...")
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3_client.delete_objects(Bucket=bucket, Delete=delete_keys)
        print(f"Pasta limpa com sucesso.")
    else:
        print("Pasta já estava vazia.")

def upload_layer(layer_name):
    layer_path = os.path.join(DATA_DIR, layer_name, "pib")
    s3_prefix = f"{layer_name}/pib"
    
    if not os.path.exists(layer_path):
        print(f"Diretório não encontrado: {layer_path}")
        return 0

    s3 = boto3.client('s3')
    
    # PASSO ESSENCIAL: Limpa a pasta antes de subir os novos arquivos
    clear_s3_prefix(s3, BUCKET_NAME, s3_prefix)
    
    print(f"Iniciando upload da camada {layer_name.upper()} para s3://{BUCKET_NAME}/{s3_prefix}")

    uploaded_count = 0
    for root, dirs, files in os.walk(layer_path):
        for file in files:
            # Filtra para não subir arquivos temporários ou lixo do sistema
            if file.endswith(".parquet") or file.endswith("_SUCCESS"):
                local_path = os.path.join(root, file)
                
                relative_path = os.path.relpath(local_path, layer_path)
                s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")

                try:
                    s3.upload_file(local_path, BUCKET_NAME, s3_path)
                    uploaded_count += 1
                except Exception as e:
                    print(f"Erro ao enviar {file}: {e}")
    
    print(f"Camada {layer_name} finalizada. Arquivos novos enviados: {uploaded_count}")
    return uploaded_count

def run():
    total_files = 0
    layers = ["gold"] # Comece testando pela Gold para ver o resultado no Power BI
    
    try:
        for layer in layers:
            total_files += upload_layer(layer)
        
        print(f"\nPipeline finalizada. Dados atualizados e limpos no S3.")
    except NoCredentialsError:
        print("Erro: Credenciais AWS não encontradas.")

if __name__ == "__main__":
    run()