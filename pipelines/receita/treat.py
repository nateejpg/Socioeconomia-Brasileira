import os
import pandas as pd

EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/receita"
    )
)

PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/receita"
    )
)

os.makedirs(PROCESSED_DIR, exist_ok=True)

def clean_receita_csv(file_path: str):

    print(f"Processing {file_path}")

    df = pd.read_csv(
        file_path,
        sep=";",
        encoding="latin1"
    )

    # Padroniza nomes das colunas
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("ã", "a")
        .str.replace("ç", "c")
        .str.replace("í", "i")
        .str.replace("é", "e")
        .str.replace("ó", "o")
        .str.replace("ú", "u")
        .str.replace("ê", "e")
        .str.replace("ô", "o")
        .str.replace("�", "")
    )

    # Colunas monetárias
    money_cols = [
        "valor_previsto_atualizado",
        "valor_lancado",
        "valor_realizado",
        "percentual_realizado"
    ]

    for col in money_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .astype(float)
            )

    # Data
    if "data_lancamento" in df.columns:
        df["data_lancamento"] = pd.to_datetime(
            df["data_lancamento"],
            format="%d/%m/%Y",
            errors="coerce"
        )

    # Ano como inteiro
    if "ano_exercicio" in df.columns:
        df["ano_exercicio"] = df["ano_exercicio"].astype(int)

    return df


def run():
    for file in os.listdir(EXTRACTED_DIR):
        if file.endswith(".csv"):
            extracted_path = os.path.join(EXTRACTED_DIR, file)
            df = clean_receita_csv(extracted_path)

            processed_path = os.path.join(
                PROCESSED_DIR,
                file.replace(".csv", "_clean.csv")
            )

            df.to_csv(
                processed_path,
                index=False
            )

            print(f"Saved {processed_path}")


if __name__ == "__main__":
    run()
