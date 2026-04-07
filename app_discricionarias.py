# app_discricionarias.py
"""
Modulo de Discricionarias e Legais para o app.py principal.
Carrega o CSV pre-filtrado gerado pelo coletor_discricionarias.py
e exibe filtros interativos + exportacao.
"""

import os
import io
import pandas as pd
import streamlit as st
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_discricionarias")
ARQUIVO  = os.path.join(DATA_DIR, "discricionarias_to.csv")


# --- CARREGAMENTO -------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner="Carregando dados de Discricionarias...")
def carregar_dados() -> pd.DataFrame:
    if not os.path.exists(ARQUIVO):
        return pd.DataFrame()

    df = pd.read_csv(ARQUIVO, sep=";", encoding="utf-8-sig", low_memory=False)

    # Garante tipos corretos
    for col in ["ano_proposta", "ano_assinatura", "ano_emenda"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["valor_global", "valor_repasse", "valor_contrapartida",
                "valor_empenhado", "valor_desembolsado", "valor_saldo_tesouro",
                "valor_custeio", "valor_investimento"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in ["dt_assinatura", "dt_inicio_vigencia", "dt_fim_vigencia"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


def formatar_brl(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# --- SIDEBAR DE FILTROS -------------------------------------------------------

def aplicar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renderiza os filtros na sidebar e retorna o DataFrame filtrado.
    Filtros equivalentes ao MARCO.xlsx.
    """
    st.sidebar.header("Filtros — Discricionarias")

    # Ano Proposta
    if "ano_proposta" in df.columns:
        anos_prop = sorted(df["ano_proposta"].dropna().unique().tolist())
        sel_anos_prop = st.sidebar.multiselect(
            "Ano Proposta",
            options=anos_prop,
            default=anos_prop,
            key="disc_ano_proposta"
        )
        if sel_anos_prop:
            df = df[df["ano_proposta"].isin(sel_anos_prop)]

    # Ano Assinatura
    if "ano_assinatura" in df.columns:
        anos_ass = sorted(df["ano_assinatura"].dropna().unique().tolist())
        sel_anos_ass = st.sidebar.multiselect(
            "Ano Assinatura",
            options=anos_ass,
            default=anos_ass,
            key="disc_ano_assinatura"
        )
        if sel_anos_ass:
            df = df[df["ano_assinatura"].isin(sel_anos_ass)]

    # Situacao
    if "situacao" in df.columns:
        situacoes = sorted(df["situacao"].dropna().unique().tolist())
        sel_sit = st.sidebar.multiselect(
            "Situacao do Convenio",
            options=situacoes,
            default=[],
            placeholder="Todas",
            key="disc_situacao"
        )
        if sel_sit:
            df = df[df["situacao"].isin(sel_sit)]

    # Municipio Beneficiario
    if "municipio_beneficiario" in df.columns:
        municipios = sorted(df["municipio_beneficiario"].dropna().unique().tolist())
        sel_mun = st.sidebar.multiselect(
            "Municipio Beneficiario",
            options=municipios,
            default=[],
            placeholder="Todos",
            key="disc_municipio"
        )
        if sel_mun:
            df = df[df["municipio_beneficiario"].isin(sel_mun)]

    # Parlamentar
    if "parlamentar" in df.columns:
        parlamentares = sorted(df["parlamentar"].dropna().unique().tolist())
        sel_parl = st.sidebar.multiselect(
            "Parlamentar",
            options=parlamentares,
            default=[],
            placeholder="Todos",
            key="disc_parlamentar"
        )
        if sel_parl:
            df = df[df["parlamentar"].isin(sel_parl)]

    # Orgao Concedente
    if "orgao_concedente" in df.columns:
        orgaos = sorted(df["orgao_concedente"].dropna().unique().tolist())
        sel_org = st.sidebar.multiselect(
            "Orgao Concedente",
            options=orgaos,
            default=[],
            placeholder="Todos",
            key="disc_orgao"
        )
        if sel_org:
            df = df[df["orgao_concedente"].isin(sel_org)]

    # Modalidade
    if "modalidade" in df.columns:
        modalidades = sorted(df["modalidade"].dropna().unique().tolist())
        sel_mod = st.sidebar.multiselect(
            "Modalidade",
            options=modalidades,
            default=[],
            placeholder="Todas",
            key="disc_modalidade"
        )
        if sel_mod:
            df = df[df["modalidade"].isin(sel_mod)]

    return df


# --- EXPORTACAO ---------------------------------------------------------------

def gerar_excel(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Discricionarias_TO")
    return buffer.getvalue()


def gerar_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(sep=";", index=False, encoding="utf-8-sig").encode("utf-8-sig")


def bloco_exportacao(df: pd.DataFrame):
    st.markdown("---")
    st.subheader("Exportar consulta filtrada")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Registros filtrados", f"{len(df):,}")

    with col2:
        nome_csv = f"discricionarias_TO_{datetime.today().strftime('%Y%m%d')}.csv"
        st.download_button(
            label="Download CSV",
            data=gerar_csv(df),
            file_name=nome_csv,
            mime="text/csv",
            use_container_width=True,
            key="disc_download_csv"
        )

    with col3:
        nome_xlsx = f"discricionarias_TO_{datetime.today().strftime('%Y%m%d')}.xlsx"
        st.download_button(
            label="Download Excel",
            data=gerar_excel(df),
            file_name=nome_xlsx,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="disc_download_excel"
        )


# --- KPIS ---------------------------------------------------------------------

def bloco_kpis(df: pd.DataFrame):
    col1, col2, col3, col4 = st.columns(4)

    total = len(df)
    val_global  = df["valor_global"].sum()       if "valor_global"  in df.columns else 0
    val_repasse = df["valor_repasse"].sum()       if "valor_repasse" in df.columns else 0
    val_custeio = df["valor_custeio"].sum()       if "valor_custeio" in df.columns else 0
    val_invest  = df["valor_investimento"].sum()  if "valor_investimento" in df.columns else 0

    col1.metric("Total Convenios",    f"{total:,}")
    col2.metric("Valor Global",        formatar_brl(val_global))
    col3.metric("Valor Custeio",       formatar_brl(val_custeio))
    col4.metric("Valor Investimento",  formatar_brl(val_invest))


# --- TABELA DETALHADA ---------------------------------------------------------

def bloco_tabela(df: pd.DataFrame):
    st.subheader("Detalhamento dos convenios")

    # Colunas prioritarias para exibicao (equivalente ao MARCO.xlsx)
    colunas_exibir = [
        c for c in [
            "nr_convenio", "nr_proposta", "ano_proposta", "ano_assinatura",
            "situacao", "modalidade", "municipio_beneficiario",
            "proponente", "orgao_concedente", "parlamentar",
            "nr_emenda", "objeto",
            "valor_global", "valor_repasse", "valor_custeio", "valor_investimento",
            "valor_empenhado", "valor_desembolsado",
            "dt_assinatura", "dt_inicio_vigencia", "dt_fim_vigencia",
        ]
        if c in df.columns
    ]

    st.dataframe(
        df[colunas_exibir].reset_index(drop=True),
        use_container_width=True,
        height=450
    )


# --- RENDER PRINCIPAL ---------------------------------------------------------

def render():
    st.title("Discricionarias e Legais — Tocantins")

    # Verifica se arquivo existe
    if not os.path.exists(ARQUIVO):
        st.error(
            "Arquivo de dados nao encontrado.\n\n"
            "Execute primeiro:\n\n"
            "```\npython coletor_discricionarias.py\n```"
        )
        return

    # Carrega dados
    df_raw = carregar_dados()

    if df_raw.empty:
        st.warning("Nenhum dado disponivel.")
        return

    # Info de atualizacao
    data_mod = datetime.fromtimestamp(os.path.getmtime(ARQUIVO))
    st.caption(
        f"Fonte: Transferegov — repositorio.dados.gov.br/seges/detru | "
        f"Arquivo gerado em: {data_mod.strftime('%d/%m/%Y %H:%M')}"
    )

    # Aplica filtros da sidebar
    df_filtrado = aplicar_filtros(df_raw)

    if df_filtrado.empty:
        st.warning("Nenhum registro encontrado com os filtros selecionados.")
        return

    # KPIs
    bloco_kpis(df_filtrado)

    st.markdown("---")

    # Tabela
    bloco_tabela(df_filtrado)

    # Exportacao
    bloco_exportacao(df_filtrado)
