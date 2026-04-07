import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ─── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard BI — Emendas Parlamentares TO",
    page_icon="📊",
    layout="wide"
)

# ─── CARGA DE DADOS ────────────────────────────────────────────────────────────
@st.cache_data
def load_emendas():
    df = pd.read_csv("emendas_to.csv", sep=";")
    df["valor_total"] = df["valor_custeio"] + df["valor_investimento"]
    df["ano_emenda"]  = df["ano_emenda"].astype(str)
    df["ano_plano"]   = df["ano_plano"].astype(str)
    return df

@st.cache_data
def load_fundo():
    df = pd.read_csv("fundo_a_fundo.csv", sep=";")
    return df

df        = load_emendas()
df_fundo  = load_fundo()

# ─── SIDEBAR — FILTROS ─────────────────────────────────────────────────────────
st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f1"
    "/Bras%C3%A3o_do_Tocantins.svg/800px-Bras%C3%A3o_do_Tocantins.svg.png",
    width=90
)
st.sidebar.title("🔎 Filtros")

anos = ["Todos"] + sorted(df["ano_emenda"].dropna().unique().tolist())
situacoes = ["Todas"] + sorted(df["situacao"].dropna().unique().tolist())
parlamentares = ["Todos"] + sorted(df["parlamentar"].dropna().unique().tolist())
municipios = ["Todos"] + sorted(df["beneficiario"].dropna().unique().tolist())

f_ano         = st.sidebar.selectbox("Ano da Emenda", anos)
f_situacao    = st.sidebar.selectbox("Situação", situacoes)
f_parlamentar = st.sidebar.selectbox("Parlamentar", parlamentares)
f_municipio   = st.sidebar.selectbox("Município Beneficiário", municipios)

# ─── APLICAR FILTROS ───────────────────────────────────────────────────────────
df_f = df.copy()

if f_ano         != "Todos":  df_f = df_f[df_f["ano_emenda"]   == f_ano]
if f_situacao    != "Todas":  df_f = df_f[df_f["situacao"]      == f_situacao]
if f_parlamentar != "Todos":  df_f = df_f[df_f["parlamentar"]   == f_parlamentar]
if f_municipio   != "Todos":  df_f = df_f[df_f["beneficiario"]  == f_municipio]

# ─── HEADER ────────────────────────────────────────────────────────────────────
st.title("📊 Dashboard BI — Emendas Parlamentares TO")
st.caption("Fonte: Transferegov | Modalidade: Transferências Especiais")
st.divider()

# ─── KPIs ──────────────────────────────────────────────────────────────────────
total_emendas       = len(df_f)
valor_custeio       = df_f["valor_custeio"].sum()
valor_investimento  = df_f["valor_investimento"].sum()
valor_total         = df_f["valor_total"].sum()

k1, k2, k3, k4 = st.columns(4)

k1.metric(
    "📋 Total de Emendas",
    f"{total_emendas:,.0f}".replace(",", ".")
)
k2.metric(
    "💰 Valor Total",
    f"R$ {valor_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)
k3.metric(
    "🏥 Valor Custeio",
    f"R$ {valor_custeio:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)
k4.metric(
    "🏗️ Valor Investimento",
    f"R$ {valor_investimento:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)

st.divider()

# ─── GRÁFICOS — LINHA 1 ────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

# Top 10 Parlamentares por quantidade
with col1:
    top_parl = (
        df_f.groupby("parlamentar")
        .size()
        .reset_index(name="qtd")
        .nlargest(10, "qtd")
        .sort_values("qtd")
    )
    fig = px.bar(
        top_parl,
        x="qtd", y="parlamentar",
        orientation="h",
        title="🏛️ Top 10 Parlamentares — Qtd. de Emendas",
        color="qtd",
        color_continuous_scale="Blues",
        labels={"qtd": "Quantidade", "parlamentar": "Parlamentar"}
    )
    fig.update_layout(coloraxis_showscale=False, yaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)

# Top 10 Municípios por valor total
with col2:
    top_mun = (
        df_f.groupby("beneficiario")["valor_total"]
        .sum()
        .reset_index()
        .nlargest(10, "valor_total")
        .sort_values("valor_total")
    )
    top_mun["valor_fmt"] = top_mun["valor_total"].apply(
        lambda x: f"R$ {x:,.0f}".replace(",", ".")
    )
    fig = px.bar(
        top_mun,
        x="valor_total", y="beneficiario",
        orientation="h",
        title="🏙️ Top 10 Municípios — Valor Total",
        color="valor_total",
        color_continuous_scale="Greens",
        labels={"valor_total": "Valor (R$)", "beneficiario": "Município"},
        text="valor_fmt"
    )
    fig.update_layout(coloraxis_showscale=False, yaxis_title=None)
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

# ─── GRÁFICOS — LINHA 2 ────────────────────────────────────────────────────────
col3, col4 = st.columns(2)

# Distribuição por Situação
with col3:
    sit = df_f.groupby("situacao").size().reset_index(name="qtd")
    fig = px.pie(
        sit,
        names="situacao", values="qtd",
        title="📌 Distribuição por Situação",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

# Evolução por Ano
with col4:
    evolucao = (
        df.groupby("ano_emenda")  # usa df completo para referência
        .agg(valor_total=("valor_total", "sum"), qtd=("valor_total", "count"))
        .reset_index()
        .sort_values("ano_emenda")
    )
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=evolucao["ano_emenda"], y=evolucao["valor_total"],
               name="Valor Total (R$)", marker_color="#1f77b4"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=evolucao["ano_emenda"], y=evolucao["qtd"],
                   name="Qtd Emendas", mode="lines+markers",
                   line=dict(color="orange", width=2)),
        secondary_y=True
    )
    fig.update_layout(title="📅 Evolução por Ano da Emenda")
    fig.update_yaxes(title_text="Valor (R$)", secondary_y=False)
    fig.update_yaxes(title_text="Quantidade", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

# ─── GRÁFICO — LINHA 3 ─────────────────────────────────────────────────────────
st.subheader("📊 Custeio vs Investimento por Ano")

custeio_inv = (
    df_f.groupby("ano_emenda")
    .agg(custeio=("valor_custeio", "sum"), investimento=("valor_investimento", "sum"))
    .reset_index()
    .sort_values("ano_emenda")
)
fig = go.Figure()
fig.add_trace(go.Bar(
    x=custeio_inv["ano_emenda"], y=custeio_inv["custeio"],
    name="Custeio", marker_color="#2ca02c"
))
fig.add_trace(go.Bar(
    x=custeio_inv["ano_emenda"], y=custeio_inv["investimento"],
    name="Investimento", marker_color="#1f77b4"
))
fig.update_layout(barmode="stack", xaxis_title="Ano", yaxis_title="Valor (R$)")
st.plotly_chart(fig, use_container_width=True)

# ─── TABELA DETALHADA ──────────────────────────────────────────────────────────
with st.expander("🔍 Ver dados detalhados"):
    st.dataframe(
        df_f[[
            "codigo_plano", "ano_emenda", "parlamentar",
            "beneficiario", "situacao", "valor_custeio",
            "valor_investimento", "valor_total"
        ]].sort_values("valor_total", ascending=False),
        use_container_width=True,
        hide_index=True
    )

# ─── RODAPÉ ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Desenvolvido por Seplan | Dados: Transferegov 🏛️")
