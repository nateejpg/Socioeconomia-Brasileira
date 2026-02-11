import os
import zipfile

EXTRACTED_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/receita"
    )
)

os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

def extrair_receita(caminho_zip):
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
            
            # Geralmente so tem 1 CSV no zip da receita
            target_file = csv_files[0]
            zip_ref.extract(target_file, EXTRACTED_FOLDER)
            
            csv_path = os.path.join(EXTRACTED_FOLDER, target_file)
            
        return csv_path

    except Exception as e:
        print(f"Erro extracao: {e}")
        return None