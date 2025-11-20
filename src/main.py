import random
import sys
import time
from pathlib import Path

import polars as pl

# --- CONFIGURAÇÃO DE ARQUITETURA ---
# Define a raiz do projeto dinamicamente
BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def setup_inicial():
    """Prepara o terreno antes de rodar."""
    print(f"🏗️  Ambiente: {sys.prefix}")
    print(f"📂 Raiz do Projeto: {BASE_DIR}")

    # Garante que as pastas existam (Idempotência)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def gerar_dados_brutos():
    """Simula a extração de dados de um sistema legado."""
    print("\n1️⃣  [EXTRACT] Gerando dados brutos...")

    # Simulando 5.000 registros de vendas
    # Quebra de linha manual para satisfazer o Linter (E501)
    dados = {
        "id_transacao": range(1, 5001),
        "filial": [
            random.choice(["SP", "RJ", "MG", "RS", "SC"]) for _ in range(5000)
        ],
        "valor_venda": [random.uniform(10.0, 1000.0) for _ in range(5000)],
        "categoria": [
            random.choice(["Eletronicos", "Moveis", "Servicos"])
            for _ in range(5000)
        ],
    }

    df = pl.DataFrame(dados)
    caminho_arquivo = RAW_DIR / "vendas_bruto.csv"
    df.write_csv(caminho_arquivo)
    print(f"   ✅ Arquivo CSV gerado: {caminho_arquivo}")


def processar_etl():
    """Transforma os dados usando a Engine do Polars."""
    print("\n2️⃣  [TRANSFORM] Iniciando processamento otimizado...")
    start_time = time.time()

    # Lazy Evaluation (Scan)
    q = (
        pl.scan_csv(RAW_DIR / "vendas_bruto.csv")
        .filter(pl.col("valor_venda") > 50)
        .with_columns(
            (pl.col("valor_venda") * 0.15).alias("imposto_estimado")
        )
        .group_by(["filial", "categoria"])
        .agg(
            [
                pl.col("valor_venda").sum().alias("faturamento_total"),
                pl.col("imposto_estimado").sum().alias("total_impostos"),
                pl.len().alias("qtd_vendas"),
            ]
        )
        .sort("faturamento_total", descending=True)
    )

    # Execução Real (Action)
    df_final = q.collect()
    tempo = time.time() - start_time

    print(f"   ⚡ Processamento concluído em {tempo:.4f} segundos.")

    print("\n3️⃣  [LOAD] Salvando resultados...")
    caminho_saida = PROCESSED_DIR / "relatorio_vendas.parquet"
    df_final.write_parquet(caminho_saida)
    print(f"   💾 Dados salvos em formato Parquet: {caminho_saida}")

    print("\n📊 PREVIEW DO RELATÓRIO FINAL:")
    print(df_final.head(5))


if __name__ == "__main__":
    setup_inicial()
    gerar_dados_brutos()
    processar_etl()
