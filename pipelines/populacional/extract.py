import os
import json

EXTRACTED_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/extracted/populacao"))

def run(raw_dir):
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    
    raw_file = os.path.join(raw_dir, "populacao_br_raw.json")
    
    if not os.path.exists(raw_file):
        raise FileNotFoundError("Arquivo raw nao encontrado.")
        
    with open(raw_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    records = data[1]
    
    extracted_file = os.path.join(EXTRACTED_DIR, "populacao_records.json")
    
    with open(extracted_file, "w", encoding="utf-8") as f:
        json.dump(records, f)
        
    return EXTRACTED_DIR