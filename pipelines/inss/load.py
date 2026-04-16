import os
import boto3

GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/inss"))
BUCKET_NAME = os.getenv("AWS_S3_BUCKET")
S3_PREFIX = "gold/fato_inss"

def run():
    if not BUCKET_NAME:
        raise ValueError("Variável de ambiente AWS_S3_BUCKET não encontrada.")

    print(f"[LOAD] A conectar ao S3 para limpeza prévia...")
    
    # 1. LIMPEZA NO S3 (Evita dados duplicados no Athena)
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(BUCKET_NAME)
    
    # Filtra todos os objetos que estão dentro da pasta fato_inss e apaga-os
    objetos_antigos = bucket.objects.filter(Prefix=f"{S3_PREFIX}/")
    apagados = objetos_antigos.delete()
    
    if apagados and apagados[0].get('Deleted'):
        print(f"[LOAD] {len(apagados[0]['Deleted'])} ficheiros antigos apagados do S3.")

    # 2. UPLOAD DOS NOVOS FICHEIROS
    print("[LOAD] A enviar os novos ficheiros Parquet...")
    s3_client = boto3.client("s3")

    for root, _, files in os.walk(GOLD_DIR):
        for file in files:
            local_path = os.path.join(root, file)
            # Garante que as barras no caminho S3 estão no formato correto (/)
            s3_key = f"{S3_PREFIX}/{os.path.relpath(local_path, GOLD_DIR)}".replace("\\", "/")
            
            s3_client.upload_file(local_path, BUCKET_NAME, s3_key)

    return f"s3://{BUCKET_NAME}/{S3_PREFIX}"