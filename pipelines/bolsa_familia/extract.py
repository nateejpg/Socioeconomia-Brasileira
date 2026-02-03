import os
import zipfile

def extrair_dados(zip_path):
    if not os.path.exists(zip_path):
        print(f"Arquivo nao encontrado: {zip_path}")
        return None

    extracted_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../data/extracted/bolsa_familia"
        )
    )
    
    os.makedirs(extracted_folder, exist_ok=True)

    print(f"Extraindo: {os.path.basename(zip_path)}")

    try:
        csv_path = None
        with zipfile.ZipFile(zip_path, "r") as zip_ref:

            file_names = zip_ref.namelist()
            csv_files = [f for f in file_names if f.lower().endswith(".csv")]
            
            if not csv_files:
                return None
            
            target_file = csv_files[0]
            zip_ref.extract(target_file, extracted_folder)
            
            csv_path = os.path.join(extracted_folder, target_file)
            
        return csv_path

    except zipfile.BadZipFile:
        print(f"ZIP invalido: {zip_path}")
        return None
    except Exception as e:
        print(f"Erro na extracao: {e}")
        return None