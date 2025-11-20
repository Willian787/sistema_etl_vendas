import pytest
from pathlib import Path
import sys

# Adiciona a raiz do projeto ao Python Path para encontrar o módulo 'src'
sys.path.append(str(Path(__file__).parent.parent))

from src.main import gerar_dados_brutos, processar_etl, RAW_DIR, PROCESSED_DIR

@pytest.fixture(autouse=True)
def setup_ambiente():
    """Garante que as pastas estao limpas ou criadas antes de testar."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def test_fluxo_completo_etl():
    """
    Teste de Integracao:
    1. Gera dados.
    2. Processa.
    3. Verifica se o arquivo de saida existe e tem conteudo.
    """
    # 1. Executa a geracao
    gerar_dados_brutos()
    arquivo_raw = RAW_DIR / "vendas_bruto.csv"
    assert arquivo_raw.exists(), "FALHA: O arquivo CSV bruto nao foi gerado!"

    # 2. Executa o processamento
    processar_etl()
    arquivo_final = PROCESSED_DIR / "relatorio_vendas.parquet"
    
    # 3. Validacoes (Asserts)
    assert arquivo_final.exists(), "FALHA: O arquivo Parquet final nao foi gerado!"
    assert arquivo_final.stat().st_size > 0, "FALHA: O arquivo Parquet esta vazio!"
