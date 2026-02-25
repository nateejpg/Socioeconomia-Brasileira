import os
import zipfile

EXTRACTED_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/extracted/orcamentos"))

def run(raw_dir):
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    
    arquivos_extraidos = 0
    
    for file in os.listdir(raw_dir):
        if file.endswith(".zip"):
            file_path = os.path.join(raw_dir, file)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(EXTRACTED_DIR)
                arquivos_extraidos += 1
                
    if arquivos_extraidos == 0:
        raise ValueError("Nenhum arquivo ZIP encontrado para extracao.")
        
    return EXTRACTED_DIR