import streamlit as st
import polars as pl
import plotly.express as px
from pathlib import Path
import sys
import time

# Adiciona a raiz ao path para conseguir importar o ETL
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

# ARQUITETO: Adicionamos 'setup_inicial' na importação
from src.main import processar_etl, gerar_dados_brutos, setup_inicial, PROCESSED_DIR

# --- GARANTIA DE INFRAESTRUTURA ---
# Antes de qualquer coisa, cria as pastas data/raw e data/processed se não existirem
setup_inicial()

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
        # Se o arquivo não existe, não mostra erro, mostra instrução
        return None
    
    # Lê o Parquet (Muito rápido)
    return pl.read_parquet(arquivo)

# --- SIDEBAR (LATERAL) ---
with st.sidebar:
    st.header("⚙️ Operações")
    # Dica visual para o usuário saber que precisa clicar aqui na primeira vez
    arquivo_existe = (PROCESSED_DIR / "relatorio_vendas.parquet").exists()
    
    if not arquivo_existe:
        st.warning("⚠️ Dados não encontrados na nuvem.")
        texto_botao = "🚀 INICIAR SISTEMA (Primeira Carga)"
    else:
        texto_botao = "🔄 Rodar ETL (Recalcular)"

    if st.button(texto_botao):
        with st.spinner("Construindo infraestrutura e processando dados..."):
            # O setup_inicial já rodou no início, mas garantimos aqui
            setup_inicial() 
            gerar_dados_brutos()
            processar_etl()
            time.sleep(1)
        st.success("Sucesso! O Sistema está online.")
        st.rerun() # Recarrega a página automaticamente

# --- CARREGA OS DADOS ---
df = carregar_dados()

if df is not None:
    # --- KPIS (INDICADORES PRINCIPAIS) ---
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
        fig_bar = px.bar(
            df.to_pandas(), 
            x="filial", 
            y="faturamento_total",
            color="categoria",
            title="Vendas por Região",
            template="plotly_dark"
        )
        st.plotly_chart(fig_bar, width="stretch") # Ajuste moderno

    with col_graf2:
        st.subheader("🍕 Distribuição de Categorias")
        fig_pie = px.pie(
            df.to_pandas(), 
            names="categoria", 
            values="faturamento_total", 
            hole=0.4,
            template="plotly_dark"
        )
        st.plotly_chart(fig_pie, width="stretch")

    # --- TABELA DE DADOS ---
    st.subheader("📋 Dados Detalhados")
    st.dataframe(df.to_pandas(), width="stretch")

else:
    # Mensagem amigável se for a primeira vez
    st.info("👋 Bem-vindo ao Sistema ETL Enterprise!")
    st.markdown("""
        **Como o sistema é novo na nuvem, os dados ainda não foram gerados.**
        
        👉 Por favor, clique no botão **'INICIAR SISTEMA'** na barra lateral esquerda para rodar a pipeline pela primeira vez.
    """)