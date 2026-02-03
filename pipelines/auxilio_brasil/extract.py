import os
import zipfile

# --- Configuração de Pastas (Igual Receita) ---
RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/auxilio_brasil"
    )
)

EXTRACTED_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/auxilio_brasil"
    )
)

os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

# --- Função para o Orquestrador (Extrai 1 arquivo) ---
def extrair_dados_auxilio(caminho_zip):
    if not os.path.exists(caminho_zip):
        return None

    print(f"Extraindo: {os.path.basename(caminho_zip)}")

    try:
        csv_path = None
        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            file_names = zip_ref.namelist()
            csv_files = [f for f in file_names if f.lower().endswith(".csv")]
            
            if not csv_files:
                return None
            
            # Extrai apenas o CSV
            target_file = csv_files[0]
            zip_ref.extract(target_file, EXTRACTED_FOLDER)
            
            csv_path = os.path.join(EXTRACTED_FOLDER, target_file)
            
        return csv_path

    except zipfile.BadZipFile:
        print(f"ZIP Inválido: {caminho_zip}")
        return None
    except Exception as e:
        print(f"Erro: {e}")
        return None

# --- Modo Script (Se rodar direto, extrai tudo igual Receita) ---
if __name__ == "__main__":
    print(f"Iniciando extração em massa de: {RAW_FOLDER}")
    
    for file_name in os.listdir(RAW_FOLDER):
        if file_name.endswith(".zip"):
            caminho_completo = os.path.join(RAW_FOLDER, file_name)
            extrair_dados_auxilio(caminho_completo)
            
    print("Extração concluída!")