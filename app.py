import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import app_discricionarias  # ← importado no TOPO, junto com os demais

# ─── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard BI — Seplan TO",
    page_icon="📊",
    layout="wide"
)

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

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_brl(valor: float) -> str:
    """Formata número no padrão R$ brasileiro."""
    return f"R$ {valor:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_int(valor: int) -> str:
    return f"{valor:,}".replace(",", ".")

# ─── CARGA DE DADOS ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@st.cache_data(show_spinner="⏳ Carregando Especiais...")
def load_emendas() -> pd.DataFrame:
    path = os.path.join(BASE_DIR, "emendas_to.csv")
    df = pd.read_csv(path, sep=";", low_memory=False)
    df["valor_total"]        = df["valor_custeio"] + df["valor_investimento"]
    df["ano_emenda"]         = df["ano_emenda"].astype(str)
    df["ano_plano"]          = df["ano_plano"].astype(str)
    if "natureza_juridica" not in df.columns:
        df["natureza_juridica"] = "Não informado"
    # Otimiza memória
    for col in ["situacao", "parlamentar", "beneficiario", "natureza_juridica", "ano_emenda"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df

@st.cache_data(show_spinner="⏳ Carregando Fundo a Fundo...")
def load_fundo() -> pd.DataFrame:
    path = os.path.join(BASE_DIR, "fundo_a_fundo.csv")
    df = pd.read_csv(path, sep=";", low_memory=False)
    df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
    df["data_fim"]    = pd.to_datetime(df["data_fim"],    errors="coerce")
    df["ano"]         = df["data_inicio"].dt.year.astype("Int64").astype(str)
    if "natureza_juridica" not in df.columns:
        df["natureza_juridica"] = "Não informado"
    # Otimiza memória
    for col in ["situacao", "sigla_orgao", "municipio", "natureza_juridica", "ano"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df

# Carrega uma única vez
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
# ABAS PRINCIPAIS — definidas UMA ÚNICA VEZ
# ═══════════════════════════════════════════════════════════════════════════════
aba1, aba2, aba3 = st.tabs([
    "⭐  Especiais",
    "📂  Discricionárias e Legais",
    "🔄  Fundo a Fundo",
])

# ───────────────────────────────────────────────────────────────────────────────
# ABA 1 — ESPECIAIS
# ───────────────────────────────────────────────────────────────────────────────
with aba1:

    # ── Filtros ───────────────────────────────────────────────────────────────
    with st.expander("🔎 Filtros", expanded=True):
        f1, f2, f3, f4, f5 = st.columns(5)

        anos_e = ["Todos"] + sorted(df_emendas["ano_emenda"].dropna().unique().tolist())
        sits_e = ["Todas"] + sorted(df_emendas["situacao"].dropna().unique().tolist())
        parls  = ["Todos"] + sorted(df_emendas["parlamentar"].dropna().unique().tolist())
        munis  = ["Todos"] + sorted(df_emendas["beneficiario"].dropna().unique().tolist())
        nats_e = ["Todas"] + sorted(df_emendas["natureza_juridica"].dropna().unique().tolist())

        f_ano   = f1.selectbox("Ano da Emenda",          anos_e, key="e_ano")
        f_sit   = f2.selectbox("Situação",               sits_e, key="e_sit")
        f_parl  = f3.selectbox("Parlamentar",            parls,  key="e_parl")
        f_muni  = f4.selectbox("Município Beneficiário", munis,  key="e_muni")
        f_nat_e = f5.selectbox("Natureza Jurídica",      nats_e, key="e_nat")

    # ── Aplicar filtros SEM .copy() desnecessário ─────────────────────────────
    mask = pd.Series(True, index=df_emendas.index)
    if f_ano   != "Todos": mask &= df_emendas["ano_emenda"]        == f_ano
    if f_sit   != "Todas": mask &= df_emendas["situacao"]          == f_sit
    if f_parl  != "Todos": mask &= df_emendas["parlamentar"]       == f_parl
    if f_muni  != "Todos": mask &= df_emendas["beneficiario"]      == f_muni
    if f_nat_e != "Todas": mask &= df_emendas["natureza_juridica"] == f_nat_e
    df_e = df_emendas[mask]  # ← view, não cópia

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total de Especiais", fmt_int(len(df_e)))
    k2.metric("💰 Valor Total",        fmt_brl(df_e["valor_total"].sum()))
    k3.metric("🏥 Custeio",            fmt_brl(df_e["valor_custeio"].sum()))
    k4.metric("🏗️ Investimento",       fmt_brl(df_e["valor_investimento"].sum()))

    st.divider()

    # ── Gráficos — Linha 1 ────────────────────────────────────────────────────
    c1, c2 = st.columns(2, gap="large")

    with c1:
        top_parl = (
            df_e.groupby("parlamentar", observed=True).size()
            .reset_index(name="qtd")
            .nlargest(10, "qtd")
            .sort_values("qtd")
        )
        fig = px.bar(
            top_parl, x="qtd", y="parlamentar", orientation="h",
            title="🏛️ Top 10 Parlamentares — Quantidade de Especiais",
            color="qtd", color_continuous_scale="Blues",
            labels={"qtd": "Quantidade", "parlamentar": ""},
            text="qtd"
        )
        fig.update_layout(coloraxis_showscale=False, height=420,
                          margin=dict(l=10, r=30, t=50, b=10))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        top_mun = (
            df_e.groupby("beneficiario", observed=True)["valor_total"].sum()
            .reset_index()
            .nlargest(10, "valor_total")
            .sort_values("valor_total")
        )
        top_mun["label"] = top_mun["valor_total"].apply(lambda x: f"R$ {x/1e6:.1f}M")
        fig = px.bar(
            top_mun, x="valor_total", y="beneficiario", orientation="h",
            title="🏙️ Top 10 Municípios — Valor Total",
            color="valor_total", color_continuous_scale="Greens",
            labels={"valor_total": "Valor (R$)", "beneficiario": ""},
            text="label"
        )
        fig.update_layout(coloraxis_showscale=False, height=420,
                          margin=dict(l=10, r=80, t=50, b=10))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── Gráficos — Linha 2 ────────────────────────────────────────────────────
    c3, c4, c5 = st.columns(3, gap="large")

    with c3:
        sit = df_e.groupby("situacao", observed=True).size().reset_index(name="qtd")
        fig = go.Figure(go.Pie(
            labels=sit["situacao"], values=sit["qtd"],
            hole=0.45, textinfo="percent",
            hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
            textposition="inside", insidetextorientation="radial",
            marker=dict(colors=px.colors.qualitative.Set2,
                        line=dict(color="white", width=2))
        ))
        fig.update_layout(
            title=dict(text="📌 Distribuição por Situação", x=0, font=dict(size=15)),
            height=420,
            legend=dict(orientation="v", x=1.02, y=0.5,
                        yanchor="middle", font=dict(size=11)),
            margin=dict(l=10, r=160, t=50, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        evolucao = (
            df_emendas.groupby("ano_emenda", observed=True)
            .agg(valor_total=("valor_total", "sum"), qtd=("valor_total", "count"))
            .reset_index().sort_values("ano_emenda")
        )
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=evolucao["ano_emenda"], y=evolucao["valor_total"],
                             name="Valor Total (R$)", marker_color="#1f77b4"),
                      secondary_y=False)
        fig.add_trace(go.Scatter(x=evolucao["ano_emenda"], y=evolucao["qtd"],
                                 name="Qtd Especiais", mode="lines+markers",
                                 line=dict(color="orange", width=2)),
                      secondary_y=True)
        fig.update_layout(title="📅 Evolução por Ano da Emenda", height=420,
                          legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                          margin=dict(l=10, r=10, t=50, b=80))
        fig.update_yaxes(title_text="Valor (R$)", secondary_y=False)
        fig.update_yaxes(title_text="Quantidade",  secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    with c5:
        nat_e = df_e.groupby("natureza_juridica", observed=True).size().reset_index(name="qtd")
        nat_e = nat_e.sort_values("qtd", ascending=False)
        total_ne = nat_e["qtd"].sum()
        nat_e["pct"] = nat_e["qtd"] / total_ne * 100
        principais_ne = nat_e[nat_e["pct"] >= 2].copy()
        outros_ne = nat_e[nat_e["pct"] < 2]["qtd"].sum()
        if outros_ne > 0:
            principais_ne = pd.concat([
                principais_ne,
                pd.DataFrame([{"natureza_juridica": "Outros", "qtd": outros_ne,
                                "pct": outros_ne / total_ne * 100}])
            ], ignore_index=True)
        fig = go.Figure(go.Pie(
            labels=principais_ne["natureza_juridica"], values=principais_ne["qtd"],
            hole=0.45, textinfo="percent",
            hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
            textposition="inside", insidetextorientation="radial",
            marker=dict(colors=px.colors.qualitative.Pastel,
                        line=dict(color="white", width=2))
        ))
        fig.update_layout(
            title=dict(text="🏢 Natureza Jurídica", x=0, font=dict(size=15)),
            height=420,
            legend=dict(orientation="v", x=1.02, y=0.5,
                        yanchor="middle", font=dict(size=10)),
            margin=dict(l=10, r=180, t=50, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Custeio vs Investimento ───────────────────────────────────────────────
    st.subheader("📊 Custeio vs Investimento por Ano")
    custeio_inv = (
        df_e.groupby("ano_emenda", observed=True)
        .agg(custeio=("valor_custeio", "sum"), investimento=("valor_investimento", "sum"))
        .reset_index().sort_values("ano_emenda")
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(x=custeio_inv["ano_emenda"], y=custeio_inv["custeio"],
                         name="Custeio",     marker_color="#2ca02c"))
    fig.add_trace(go.Bar(x=custeio_inv["ano_emenda"], y=custeio_inv["investimento"],
                         name="Investimento", marker_color="#1f77b4"))
    fig.update_layout(barmode="stack", height=380,
                      xaxis_title="Ano", yaxis_title="Valor (R$)",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02),
                      margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # ── Tabela ────────────────────────────────────────────────────────────────
    with st.expander("🔍 Dados detalhados"):
        st.dataframe(
            df_e[[
                "codigo_plano", "ano_emenda", "parlamentar", "beneficiario",
                "natureza_juridica", "situacao",
                "valor_custeio", "valor_investimento", "valor_total"
            ]].sort_values("valor_total", ascending=False),
            use_container_width=True,
            hide_index=True
        )

    st.download_button(
        label="📥 Baixar CSV filtrado",
        data=df_e.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig"),
        file_name="especiais_to_filtrado.csv",
        mime="text/csv",
        type="primary"
    )

# ───────────────────────────────────────────────────────────────────────────────
# ABA 2 — DISCRICIONÁRIAS E LEGAIS
# ───────────────────────────────────────────────────────────────────────────────
with aba2:
    app_discricionarias.render()

# ───────────────────────────────────────────────────────────────────────────────
# ABA 3 — FUNDO A FUNDO
# ───────────────────────────────────────────────────────────────────────────────
with aba3:

    # ── Filtros ───────────────────────────────────────────────────────────────
    with st.expander("🔎 Filtros", expanded=True):
        g1, g2, g3, g4 = st.columns(4)

        anos_f = ["Todos"] + sorted(df_fundo["ano"].dropna().unique().tolist())
        sits_f = ["Todas"] + sorted(df_fundo["situacao"].dropna().unique().tolist())
        orgaos = ["Todos"] + sorted(df_fundo["sigla_orgao"].dropna().unique().tolist())
        nats_f = ["Todas"] + sorted(df_fundo["natureza_juridica"].dropna().unique().tolist())

        gf_ano = g1.selectbox("Ano",               anos_f, key="f_ano")
        gf_sit = g2.selectbox("Situação",          sits_f, key="f_sit")
        gf_org = g3.selectbox("Órgão Repassador",  orgaos, key="f_org")
        gf_nat = g4.selectbox("Natureza Jurídica", nats_f, key="f_nat")

    # ── Aplicar filtros via máscara ───────────────────────────────────────────
    mask_f = pd.Series(True, index=df_fundo.index)
    if gf_ano != "Todos": mask_f &= df_fundo["ano"]              == gf_ano
    if gf_sit != "Todas": mask_f &= df_fundo["situacao"]         == gf_sit
    if gf_org != "Todos": mask_f &= df_fundo["sigla_orgao"]      == gf_org
    if gf_nat != "Todas": mask_f &= df_fundo["natureza_juridica"] == gf_nat
    df_f2 = df_fundo[mask_f]  # ← view, não cópia

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total de Planos",   fmt_int(len(df_f2)))
    k2.metric("💰 Valor Total Plano", fmt_brl(df_f2["valor_total_plano"].sum()))
    k3.metric("📥 Total Repasse",     fmt_brl(df_f2["valor_total_repasse"].sum()))
    k4.metric("💳 Saldo Disponível",  fmt_brl(df_f2["saldo_disponivel"].sum()))

    st.divider()

    # ── Gráficos — Linha 1 ────────────────────────────────────────────────────
    d1, d2 = st.columns(2, gap="large")

    with d1:
        top_org = (
            df_f2.groupby("sigla_orgao", observed=True)["valor_total_repasse"].sum()
            .reset_index()
            .nlargest(10, "valor_total_repasse")
            .sort_values("valor_total_repasse")
        )
        top_org["label"] = top_org["valor_total_repasse"].apply(lambda x: f"R$ {x/1e6:.1f}M")
        fig = px.bar(
            top_org, x="valor_total_repasse", y="sigla_orgao", orientation="h",
            title="🏦 Top Órgãos Repassadores — Valor Total",
            color="valor_total_repasse", color_continuous_scale="Blues",
            labels={"valor_total_repasse": "Valor (R$)", "sigla_orgao": ""},
            text="label"
        )
        fig.update_layout(coloraxis_showscale=False, height=420,
                          margin=dict(l=10, r=80, t=50, b=10))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with d2:
        top_mun_f = (
            df_f2.groupby("municipio", observed=True)["valor_total_repasse"].sum()
            .reset_index()
            .nlargest(10, "valor_total_repasse")
            .sort_values("valor_total_repasse")
        )
        top_mun_f["label"] = top_mun_f["valor_total_repasse"].apply(lambda x: f"R$ {x/1e6:.1f}M")
        fig = px.bar(
            top_mun_f, x="valor_total_repasse", y="municipio", orientation="h",
            title="🏙️ Top 10 Municípios — Valor Total Repasse",
            color="valor_total_repasse", color_continuous_scale="Greens",
            labels={"valor_total_repasse": "Valor (R$)", "municipio": ""},
            text="label"
        )
        fig.update_layout(coloraxis_showscale=False, height=420,
                          margin=dict(l=10, r=80, t=50, b=10))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── Gráficos — Linha 2 ────────────────────────────────────────────────────
    d3, d4, d5 = st.columns(3, gap="large")

    with d3:
        sit_f = (df_f2.groupby("situacao", observed=True).size()
                 .reset_index(name="qtd").sort_values("qtd", ascending=False))
        total_sf = sit_f["qtd"].sum()
        sit_f["pct"] = sit_f["qtd"] / total_sf * 100
        principais_sf = sit_f[sit_f["pct"] >= 2].copy()
        outros_sf = sit_f[sit_f["pct"] < 2]["qtd"].sum()
        if outros_sf > 0:
            principais_sf = pd.concat([
                principais_sf,
                pd.DataFrame([{"situacao": "Outros", "qtd": outros_sf,
                                "pct": outros_sf / total_sf * 100}])
            ], ignore_index=True)
        cores = (px.colors.qualitative.Pastel
                 + px.colors.qualitative.Set3
                 + px.colors.qualitative.Pastel1)
        fig = go.Figure(go.Pie(
            labels=principais_sf["situacao"], values=principais_sf["qtd"],
            hole=0.45, textinfo="percent",
            hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
            textposition="inside", insidetextorientation="radial",
            marker=dict(colors=cores[:len(principais_sf)],
                        line=dict(color="white", width=2))
        ))
        fig.update_layout(
            title=dict(text="📌 Distribuição por Situação", x=0, font=dict(size=15)),
            height=420,
            legend=dict(orientation="v", x=1.02, y=0.5,
                        yanchor="middle", font=dict(size=11), itemsizing="constant"),
            margin=dict(l=10, r=180, t=50, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

    with d4:
        evolucao_f = (
            df_fundo.groupby("ano", observed=True)
            .agg(valor=("valor_total_repasse", "sum"), qtd=("valor_total_repasse", "count"))
            .reset_index().sort_values("ano")
        )
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=evolucao_f["ano"], y=evolucao_f["valor"],
                             name="Valor Repasse (R$)", marker_color="#9467bd"),
                      secondary_y=False)
        fig.add_trace(go.Scatter(x=evolucao_f["ano"], y=evolucao_f["qtd"],
                                 name="Qtd Planos", mode="lines+markers",
                                 line=dict(color="orange", width=2)),
                      secondary_y=True)
        fig.update_layout(title="📅 Evolução por Ano", height=420,
                          legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                          margin=dict(l=10, r=10, t=50, b=80))
        fig.update_yaxes(title_text="Valor (R$)", secondary_y=False)
        fig.update_yaxes(title_text="Quantidade",  secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    with d5:
        nat_f = (df_f2.groupby("natureza_juridica", observed=True).size()
                 .reset_index(name="qtd").sort_values("qtd", ascending=False))
        total_nf = nat_f["qtd"].sum()
        nat_f["pct"] = nat_f["qtd"] / total_nf * 100
        principais_nf = nat_f[nat_f["pct"] >= 2].copy()
        outros_nf = nat_f[nat_f["pct"] < 2]["qtd"].sum()
        if outros_nf > 0:
            principais_nf = pd.concat([
                principais_nf,
                pd.DataFrame([{"natureza_juridica": "Outros", "qtd": outros_nf,
                                "pct": outros_nf / total_nf * 100}])
            ], ignore_index=True)
        fig = go.Figure(go.Pie(
            labels=principais_nf["natureza_juridica"], values=principais_nf["qtd"],
            hole=0.45, textinfo="percent",
            hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
            textposition="inside", insidetextorientation="radial",
            marker=dict(colors=px.colors.qualitative.Pastel,
                        line=dict(color="white", width=2))
        ))
        fig.update_layout(
            title=dict(text="🏢 Natureza Jurídica", x=0, font=dict(size=15)),
            height=420,
            legend=dict(orientation="v", x=1.02, y=0.5,
                        yanchor="middle", font=dict(size=10)),
            margin=dict(l=10, r=180, t=50, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Composição do Repasse ─────────────────────────────────────────────────
    st.subheader("📊 Composição do Repasse por Órgão")
    comp = (
        df_f2.groupby("sigla_orgao", observed=True)
        .agg(emenda=("valor_emenda", "sum"),
             especifico=("valor_especifico", "sum"),
             voluntario=("valor_voluntario", "sum"))
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
    fig.update_layout(barmode="stack", height=380,
                      xaxis_title="Órgão", yaxis_title="Valor (R$)",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02),
                      margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # ── Tabela ────────────────────────────────────────────────────────────────
    with st.expander("🔍 Dados detalhados"):
        st.dataframe(
            df_f2[[
                "codigo_plano", "situacao", "municipio", "natureza_juridica",
                "sigla_orgao", "fundo_repassador",
                "valor_emenda", "valor_total_repasse", "valor_total_plano", "saldo_disponivel"
            ]].sort_values("valor_total_plano", ascending=False),
            use_container_width=True,
            hide_index=True
        )

    st.download_button(
        label="📥 Baixar CSV filtrado",
        data=df_f2.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig"),
        file_name="fundo_a_fundo_to_filtrado.csv",
        mime="text/csv",
        type="primary"
    )

# ─── RODAPÉ ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Dashboard desenvolvido pela Seplan-TO · Dados: Transferegov")