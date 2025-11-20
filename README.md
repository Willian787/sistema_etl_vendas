# 📊 Sistema de ETL & Dashboard de Vendas (Enterprise Grade)

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Polars](https://img.shields.io/badge/Engine-Polars_(Rust)-orange)
![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-red)
![Status](https://img.shields.io/badge/Status-Production-green)

Um sistema completo de Engenharia de Dados que demonstra uma arquitetura moderna focada em alta performance e visualização de dados.

## 🚀 A Arquitetura

O projeto foi desenhado seguindo princípios de **Clean Architecture**:

1.  **Extract (Extração):** Simulação de ingestão de dados brutos (CSV) de sistemas legados.
2.  **Transform (Transformação):** Processamento utilizando **Polars** (escrito em Rust) para performance extrema (Zero-Copy memory). Regras de negócio:
    *   Filtragem de vendas irrelevantes.
    *   Cálculo de impostos em tempo real.
    *   Agregação por filial e categoria.
3.  **Load (Carga):** Armazenamento otimizado em formato **Parquet** (Big Data).
4.  **Visualization:** Dashboard interativo via **Streamlit** com cache inteligente.

## 🛠️ Tecnologias Utilizadas

*   **Linguagem:** Python 3.12+
*   **Data Engine:** Polars (LTS-CPU optimized)
*   **Validação:** Pydantic V2
*   **Frontend:** Streamlit & Plotly
*   **Qualidade de Código:** Ruff & Pytest

## 📱 Como executar localmente

1. Clone o repositório:
   ```bash
   git clone https://github.com/Willian787/sistema_etl_vendas.git