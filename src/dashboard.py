import streamlit as st
import polars as pl
import plotly.express as px
from pathlib import Path
import sys
import time

# Adiciona a raiz ao path para conseguir importar o ETL
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

from src.main import processar_etl, gerar_dados_brutos, PROCESSED_DIR

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Dashboard de Vendas",
    page_icon="📊",
    layout="wide"
)

# --- TÍTULO E CABEÇALHO ---
st.title("📊 Painel Executivo de Vendas")
st.markdown("---")

# --- FUNÇÃO DE CARREGAMENTO OTIMIZADO ---
def carregar_dados():
    arquivo = PROCESSED_DIR / "relatorio_vendas.parquet"
    if not arquivo.exists():
        st.warning("⚠️ Arquivo de dados não encontrado. Rode o ETL primeiro!")
        return None
    
    # Lê o Parquet (Muito rápido)
    return pl.read_parquet(arquivo)

# --- SIDEBAR (LATERAL) ---
with st.sidebar:
    st.header("⚙️ Operações")
    if st.button("🔄 Rodar ETL (Recalcular)"):
        with st.spinner("Processando dados brutos..."):
            gerar_dados_brutos() # Gera novos dados aleatórios
            processar_etl()      # Processa
            time.sleep(1)        # Pequena pausa visual
        st.success("Dados atualizados com sucesso!")
        st.cache_data.clear()    # Limpa o cache para forçar recarregamento

# --- CARREGA OS DADOS ---
df = carregar_dados()

if df is not None:
    # --- KPIS (INDICADORES PRINCIPAIS) ---
    # Cálculos rápidos usando Polars
    total_vendas = df["faturamento_total"].sum()
    total_impostos = df["total_impostos"].sum()
    qtd_transacoes = df["qtd_vendas"].sum()

    col1, col2, col3 = st.columns(3)
    col1.metric("Faturamento Total", f"R$ {total_vendas:,.2f}")
    col2.metric("Impostos Recolhidos", f"R$ {total_impostos:,.2f}")
    col3.metric("Transações", f"{qtd_transacoes}")

    st.markdown("---")

    # --- GRÁFICOS (PLOTLY) ---
    col_graf1, col_graf2 = st.columns(2)

    with col_graf1:
        st.subheader("🏆 Faturamento por Filial")
        # Convertendo para Pandas apenas para o Plotly (Plotly ainda prefere Pandas/Listas)
        fig_bar = px.bar(
            df.to_pandas(), 
            x="filial", 
            y="faturamento_total",
            color="categoria",
            title="Vendas por Região e Categoria",
            template="plotly_dark"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_graf2:
        st.subheader("🍕 Distribuição de Categorias")
        fig_pie = px.pie(
            df.to_pandas(), 
            names="categoria", 
            values="faturamento_total", 
            hole=0.4,
            template="plotly_dark"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- TABELA DE DADOS ---
    st.subheader("📋 Dados Detalhados")
    st.dataframe(df.to_pandas(), use_container_width=True)