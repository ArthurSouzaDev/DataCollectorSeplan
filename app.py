import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ─── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard BI — Seplan TO",
    page_icon="📊",
    layout="wide"
)

# CSS para corrigir cortes e responsividade
st.markdown("""
    <style>
        .block-container { padding: 1.5rem 2rem 2rem 2rem; }
        div[data-testid="metric-container"] {
            background-color: #f0f4f9;
            border: 1px solid #d0dbe7;
            border-radius: 10px;
            padding: 15px 20px;
        }
        div[data-testid="metric-container"] label {
            font-size: 0.85rem !important;
        }
        .stTabs [data-baseweb="tab"] {
            font-size: 1rem;
            font-weight: 600;
            padding: 10px 24px;
        }
    </style>
""", unsafe_allow_html=True)

# ─── CARGA DE DADOS ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@st.cache_data
def load_emendas():
    path = os.path.join(BASE_DIR, "emendas_to.csv")
    df = pd.read_csv(path, sep=";")
    df["valor_total"]  = df["valor_custeio"] + df["valor_investimento"]
    df["ano_emenda"]   = df["ano_emenda"].astype(str)
    df["ano_plano"]    = df["ano_plano"].astype(str)
    return df

@st.cache_data
def load_fundo():
    path = os.path.join(BASE_DIR, "fundo_a_fundo.csv")
    df = pd.read_csv(path, sep=";")
    df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
    df["data_fim"]    = pd.to_datetime(df["data_fim"],    errors="coerce")
    df["ano"]         = df["data_inicio"].dt.year.astype("Int64").astype(str)
    return df

df_emendas = load_emendas()
df_fundo   = load_fundo()

# ─── HEADER ────────────────────────────────────────────────────────────────────
col_logo, col_title = st.columns([1, 11])
with col_logo:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f1"
        "/Bras%C3%A3o_do_Tocantins.svg/800px-Bras%C3%A3o_do_Tocantins.svg.png",
        width=60
    )
with col_title:
    st.title("Dashboard BI — Transferências Parlamentares TO")
    st.caption("Fonte: Transferegov · Secretaria de Planejamento do Tocantins")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# ABAS PRINCIPAIS
# ═══════════════════════════════════════════════════════════════════════════════
aba1, aba2 = st.tabs([
    "🏛️  Emendas Parlamentares",
    "🔄  Fundo a Fundo"
])

# ───────────────────────────────────────────────────────────────────────────────
# ABA 1 — EMENDAS PARLAMENTARES
# ───────────────────────────────────────────────────────────────────────────────
with aba1:

    # ── Filtros ──────────────────────────────────────────────────────────────
    with st.expander("🔎 Filtros", expanded=True):
        f1, f2, f3, f4 = st.columns(4)

        anos_e = ["Todos"] + sorted(df_emendas["ano_emenda"].dropna().unique().tolist())
        sits_e = ["Todas"] + sorted(df_emendas["situacao"].dropna().unique().tolist())
        parls  = ["Todos"] + sorted(df_emendas["parlamentar"].dropna().unique().tolist())
        munis  = ["Todos"] + sorted(df_emendas["beneficiario"].dropna().unique().tolist())

        f_ano  = f1.selectbox("Ano da Emenda",        anos_e, key="e_ano")
        f_sit  = f2.selectbox("Situação",             sits_e, key="e_sit")
        f_parl = f3.selectbox("Parlamentar",          parls,  key="e_parl")
        f_muni = f4.selectbox("Município Beneficiário", munis, key="e_muni")

    # ── Aplicar filtros ───────────────────────────────────────────────────────
    df_e = df_emendas.copy()
    if f_ano  != "Todos": df_e = df_e[df_e["ano_emenda"]  == f_ano]
    if f_sit  != "Todas": df_e = df_e[df_e["situacao"]    == f_sit]
    if f_parl != "Todos": df_e = df_e[df_e["parlamentar"] == f_parl]
    if f_muni != "Todos": df_e = df_e[df_e["beneficiario"]== f_muni]

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total de Emendas",    f"{len(df_e):,}".replace(",", "."))
    k2.metric("💰 Valor Total",         f"R$ {df_e['valor_total'].sum():,.0f}".replace(",","X").replace(".",",").replace("X","."))
    k3.metric("🏥 Custeio",             f"R$ {df_e['valor_custeio'].sum():,.0f}".replace(",","X").replace(".",",").replace("X","."))
    k4.metric("🏗️ Investimento",        f"R$ {df_e['valor_investimento'].sum():,.0f}".replace(",","X").replace(".",",").replace("X","."))

    st.divider()

    # ── Gráficos — Linha 1 ────────────────────────────────────────────────────
    c1, c2 = st.columns(2, gap="large")

    with c1:
        top_parl = (
            df_e.groupby("parlamentar").size()
            .reset_index(name="qtd")
            .nlargest(10, "qtd")
            .sort_values("qtd")
        )
        fig = px.bar(
            top_parl, x="qtd", y="parlamentar", orientation="h",
            title="🏛️ Top 10 Parlamentares — Quantidade de Emendas",
            color="qtd", color_continuous_scale="Blues",
            labels={"qtd": "Quantidade", "parlamentar": ""},
            text="qtd"
        )
        fig.update_layout(
            coloraxis_showscale=False,
            height=420,
            margin=dict(l=10, r=30, t=50, b=10)
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        top_mun = (
            df_e.groupby("beneficiario")["valor_total"].sum()
            .reset_index()
            .nlargest(10, "valor_total")
            .sort_values("valor_total")
        )
        top_mun["label"] = top_mun["valor_total"].apply(
            lambda x: f"R$ {x/1e6:.1f}M"
        )
        fig = px.bar(
            top_mun, x="valor_total", y="beneficiario", orientation="h",
            title="🏙️ Top 10 Municípios — Valor Total",
            color="valor_total", color_continuous_scale="Greens",
            labels={"valor_total": "Valor (R$)", "beneficiario": ""},
            text="label"
        )
        fig.update_layout(
            coloraxis_showscale=False,
            height=420,
            margin=dict(l=10, r=80, t=50, b=10)
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── Gráficos — Linha 2 ────────────────────────────────────────────────────
    c3, c4 = st.columns(2, gap="large")

with c3:
    sit = df_e.groupby("situacao").size().reset_index(name="qtd")

    fig = go.Figure(go.Pie(
        labels=sit["situacao"],
        values=sit["qtd"],
        hole=0.45,
        textinfo="percent",
        hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
        textposition="inside",
        insidetextorientation="radial",
        marker=dict(
            colors=px.colors.qualitative.Set2,
            line=dict(color="white", width=2)
        )
    ))
    fig.update_layout(
        title=dict(text="📌 Distribuição por Situação", x=0, font=dict(size=15)),
        height=420,
        legend=dict(
            orientation="v", x=1.02, y=0.5,
            yanchor="middle", font=dict(size=11)
        ),
        margin=dict(l=10, r=160, t=50, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

    with c4:
        evolucao = (
            df_emendas.groupby("ano_emenda")
            .agg(valor_total=("valor_total","sum"), qtd=("valor_total","count"))
            .reset_index().sort_values("ano_emenda")
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
        fig.update_layout(
            title="📅 Evolução por Ano da Emenda",
            height=420,
            legend=dict(orientation="h", yanchor="bottom", y=-0.25),
            margin=dict(l=10, r=10, t=50, b=80)
        )
        fig.update_yaxes(title_text="Valor (R$)", secondary_y=False)
        fig.update_yaxes(title_text="Quantidade",  secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    # ── Custeio vs Investimento ───────────────────────────────────────────────
    st.subheader("📊 Custeio vs Investimento por Ano")
    custeio_inv = (
        df_e.groupby("ano_emenda")
        .agg(custeio=("valor_custeio","sum"), investimento=("valor_investimento","sum"))
        .reset_index().sort_values("ano_emenda")
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
    fig.update_layout(
        barmode="stack",
        height=380,
        xaxis_title="Ano",
        yaxis_title="Valor (R$)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=10, r=10, t=30, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Tabela ────────────────────────────────────────────────────────────────
    with st.expander("🔍 Dados detalhados"):
        st.dataframe(
            df_e[[
                "codigo_plano","ano_emenda","parlamentar",
                "beneficiario","situacao",
                "valor_custeio","valor_investimento","valor_total"
            ]].sort_values("valor_total", ascending=False),
            use_container_width=True,
            hide_index=True
        )

# ───────────────────────────────────────────────────────────────────────────────
# ABA 2 — FUNDO A FUNDO
# ───────────────────────────────────────────────────────────────────────────────
with aba2:

    # ── Filtros ──────────────────────────────────────────────────────────────
    with st.expander("🔎 Filtros", expanded=True):
        g1, g2, g3 = st.columns(3)

        anos_f  = ["Todos"] + sorted(df_fundo["ano"].dropna().unique().tolist())
        sits_f  = ["Todas"] + sorted(df_fundo["situacao"].dropna().unique().tolist())
        orgaos  = ["Todos"] + sorted(df_fundo["sigla_orgao"].dropna().unique().tolist())

        gf_ano  = g1.selectbox("Ano",            anos_f, key="f_ano")
        gf_sit  = g2.selectbox("Situação",       sits_f, key="f_sit")
        gf_org  = g3.selectbox("Órgão Repassador", orgaos, key="f_org")

    # ── Aplicar filtros ───────────────────────────────────────────────────────
    df_f2 = df_fundo.copy()
    if gf_ano != "Todos": df_f2 = df_f2[df_f2["ano"]         == gf_ano]
    if gf_sit != "Todas": df_f2 = df_f2[df_f2["situacao"]    == gf_sit]
    if gf_org != "Todos": df_f2 = df_f2[df_f2["sigla_orgao"] == gf_org]

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total de Planos",     f"{len(df_f2):,}".replace(",","."))
    k2.metric("💰 Valor Total Plano",   f"R$ {df_f2['valor_total_plano'].sum():,.0f}".replace(",","X").replace(".",",").replace("X","."))
    k3.metric("📥 Total Repasse",       f"R$ {df_f2['valor_total_repasse'].sum():,.0f}".replace(",","X").replace(".",",").replace("X","."))
    k4.metric("💳 Saldo Disponível",    f"R$ {df_f2['saldo_disponivel'].sum():,.0f}".replace(",","X").replace(".",",").replace("X","."))

    st.divider()

    # ── Gráficos — Linha 1 ────────────────────────────────────────────────────
    d1, d2 = st.columns(2, gap="large")

    with d1:
        top_org = (
            df_f2.groupby("sigla_orgao")["valor_total_repasse"].sum()
            .reset_index()
            .nlargest(10, "valor_total_repasse")
            .sort_values("valor_total_repasse")
        )
        top_org["label"] = top_org["valor_total_repasse"].apply(
            lambda x: f"R$ {x/1e6:.1f}M"
        )
        fig = px.bar(
            top_org, x="valor_total_repasse", y="sigla_orgao", orientation="h",
            title="🏦 Top Órgãos Repassadores — Valor Total",
            color="valor_total_repasse", color_continuous_scale="Blues",
            labels={"valor_total_repasse": "Valor (R$)", "sigla_orgao": ""},
            text="label"
        )
        fig.update_layout(
            coloraxis_showscale=False, height=420,
            margin=dict(l=10, r=80, t=50, b=10)
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with d2:
        top_mun_f = (
            df_f2.groupby("municipio")["valor_total_repasse"].sum()
            .reset_index()
            .nlargest(10, "valor_total_repasse")
            .sort_values("valor_total_repasse")
        )
        top_mun_f["label"] = top_mun_f["valor_total_repasse"].apply(
            lambda x: f"R$ {x/1e6:.1f}M"
        )
        fig = px.bar(
            top_mun_f, x="valor_total_repasse", y="municipio", orientation="h",
            title="🏙️ Top 10 Municípios — Valor Total Repasse",
            color="valor_total_repasse", color_continuous_scale="Greens",
            labels={"valor_total_repasse": "Valor (R$)", "municipio": ""},
            text="label"
        )
        fig.update_layout(
            coloraxis_showscale=False, height=420,
            margin=dict(l=10, r=80, t=50, b=10)
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── Gráficos — Linha 2 ────────────────────────────────────────────────────
    d3, d4 = st.columns(2, gap="large")

with d3:
    sit_f = df_f2.groupby("situacao").size().reset_index(name="qtd").sort_values("qtd", ascending=False)

    # Agrupa situações com menos de 2% do total em "Outros"
    total = sit_f["qtd"].sum()
    sit_f["pct"] = sit_f["qtd"] / total * 100
    principais = sit_f[sit_f["pct"] >= 2].copy()
    outros_qtd = sit_f[sit_f["pct"] < 2]["qtd"].sum()

    if outros_qtd > 0:
        outros_row = pd.DataFrame([{"situacao": "Outros", "qtd": outros_qtd, "pct": outros_qtd/total*100}])
        principais = pd.concat([principais, outros_row], ignore_index=True)

    # Paleta com cores suficientes
    cores = (
        px.colors.qualitative.Pastel
        + px.colors.qualitative.Set3
        + px.colors.qualitative.Pastel1
    )

    fig = go.Figure(go.Pie(
        labels=principais["situacao"],
        values=principais["qtd"],
        hole=0.45,
        textinfo="percent",
        hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
        textposition="inside",
        insidetextorientation="radial",
        marker=dict(
            colors=cores[:len(principais)],
            line=dict(color="white", width=2)
        )
    ))
    fig.update_layout(
        title=dict(text="📌 Distribuição por Situação", x=0, font=dict(size=15)),
        height=420,
        legend=dict(
            orientation="v", x=1.02, y=0.5,
            yanchor="middle", font=dict(size=11),
            itemsizing="constant"
        ),
        margin=dict(l=10, r=180, t=50, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

    with d4:
        evolucao_f = (
            df_fundo.groupby("ano")
            .agg(valor=("valor_total_repasse","sum"), qtd=("valor_total_repasse","count"))
            .reset_index().sort_values("ano")
        )
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=evolucao_f["ano"], y=evolucao_f["valor"],
                   name="Valor Repasse (R$)", marker_color="#9467bd"),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=evolucao_f["ano"], y=evolucao_f["qtd"],
                       name="Qtd Planos", mode="lines+markers",
                       line=dict(color="orange", width=2)),
            secondary_y=True
        )
        fig.update_layout(
            title="📅 Evolução por Ano",
            height=420,
            legend=dict(orientation="h", yanchor="bottom", y=-0.25),
            margin=dict(l=10, r=10, t=50, b=80)
        )
        fig.update_yaxes(title_text="Valor (R$)",  secondary_y=False)
        fig.update_yaxes(title_text="Quantidade",  secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    # ── Composição do Repasse ─────────────────────────────────────────────────
    st.subheader("📊 Composição do Repasse por Órgão")
    comp = (
        df_f2.groupby("sigla_orgao")
        .agg(
            emenda    =("valor_emenda",     "sum"),
            especifico=("valor_especifico", "sum"),
            voluntario=("valor_voluntario", "sum")
        )
        .reset_index()
        .sort_values("emenda", ascending=False)
        .head(15)
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(x=comp["sigla_orgao"], y=comp["emenda"],
                         name="Emenda",     marker_color="#1f77b4"))
    fig.add_trace(go.Bar(x=comp["sigla_orgao"], y=comp["especifico"],
                         name="Específico", marker_color="#ff7f0e"))
    fig.add_trace(go.Bar(x=comp["sigla_orgao"], y=comp["voluntario"],
                         name="Voluntário", marker_color="#2ca02c"))
    fig.update_layout(
        barmode="stack", height=380,
        xaxis_title="Órgão", yaxis_title="Valor (R$)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=10, r=10, t=30, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Tabela ────────────────────────────────────────────────────────────────
    with st.expander("🔍 Dados detalhados"):
        st.dataframe(
            df_f2[[
                "codigo_plano","situacao","municipio","sigla_orgao",
                "fundo_repassador","valor_emenda",
                "valor_total_repasse","valor_total_plano","saldo_disponivel"
            ]].sort_values("valor_total_plano", ascending=False),
            use_container_width=True,
            hide_index=True
        )

# ─── RODAPÉ ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("📊 Desenvolvido por Seplan TO · Dados: Transferegov")
