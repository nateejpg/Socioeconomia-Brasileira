import os
import pandas as pd

PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/receita"
    )
)

AGG_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/aggregated/receita"
    )
)

os.makedirs(AGG_DIR, exist_ok=True)


def run():
    dfs = []

    for file in os.listdir(PROCESSED_DIR):
        if file.endswith("_clean.csv"):
            path = os.path.join(PROCESSED_DIR, file)
            dfs.append(pd.read_csv(path, parse_dates=["data_lancamento"]))

    df = pd.concat(dfs, ignore_index=True)

    # ==========================
    # Receita mensal
    # ==========================
    df["ano"] = df["data_lancamento"].dt.year
    df["mes"] = df["data_lancamento"].dt.month

    receita_mensal = (
        df.groupby(["ano", "mes"], as_index=False)
          .agg(receita_total=("valor_realizado", "sum"))
          .sort_values(["ano", "mes"])
    )

    receita_mensal.to_csv(
        os.path.join(AGG_DIR, "receita_mensal.csv"),
        index=False
    )

    # ==========================
    # Receita anual
    # ==========================
    receita_anual = (
        df.groupby("ano_exercicio", as_index=False)
          .agg(receita_total=("valor_realizado", "sum"))
          .sort_values("ano_exercicio")
    )

    receita_anual.to_csv(
        os.path.join(AGG_DIR, "receita_anual.csv"),
        index=False
    )

    print("Aggregation finished")


if __name__ == "__main__":
    run()
