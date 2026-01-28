import os
import zipfile

RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/receita"
    )
)

EXTRACTED_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/receita"
    )
)

os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

for file_name in os.listdir(RAW_FOLDER):
    if not file_name.endswith(".zip"):
        continue

    # Ex: receita_2013.zip -> 2013
    year = file_name.replace("receita_", "").replace(".zip", "")

    zip_path = os.path.join(RAW_FOLDER, file_name)

    print(f"Extraindo {file_name} para {EXTRACTED_FOLDER}")

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(EXTRACTED_FOLDER)
    except zipfile.BadZipFile:
        print(f"ZIP inválido: {file_name}")

print("Extração concluída com sucesso!")
