import os
import boto3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))
BUCKET_NAME = "socioeconomia-brasil-gold" # Certifique-se que é o seu bucket correto

def clear_s3_prefix(s3_client, prefix):
    print(f"-> Limpando pasta antiga no S3: s3://{BUCKET_NAME}/{prefix}")
    objects_to_delete = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3_client.delete_objects(Bucket=BUCKET_NAME, Delete=delete_keys)

def upload_layer(layer_name):
    layer_path = os.path.join(DATA_DIR, layer_name, "desemprego")
    s3_prefix = f"{layer_name}/desemprego"
    
    if not os.path.exists(layer_path):
        return 0

    s3 = boto3.client('s3')
    clear_s3_prefix(s3, s3_prefix)

    uploaded_count = 0
    for root, dirs, files in os.walk(layer_path):
        for file in files:
            if file.endswith(".parquet") or file.endswith("_SUCCESS"):
                local_path = os.path.join(root, file)
                s3_path = f"{s3_prefix}/{file}"
                s3.upload_file(local_path, BUCKET_NAME, s3_path)
                uploaded_count += 1
    return uploaded_count

def run():
    print("Iniciando Upload: Desemprego...")
    layers = ["bronze", "silver", "gold"]
    total = sum(upload_layer(layer) for layer in layers)
    print(f"-> Upload Desemprego concluído. {total} arquivos enviados.")

if __name__ == "__main__":
    run()