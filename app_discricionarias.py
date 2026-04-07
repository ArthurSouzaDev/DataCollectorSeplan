# app_discricionarias.py
import os
import sys
import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CSV_PATH  = os.path.join(BASE_DIR, "data_discricionarias", "discricionarias_to.csv")


# ─── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_brl(v: float) -> str:
    return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


def executar_coletor():
    """Importa e executa o coletor diretamente no mesmo processo Python."""
    if BASE_DIR not in sys.path:
        sys.path.insert(0, BASE_DIR)

    with st.status("⏳ Coletando dados do Transferegov...", expanded=True) as status:
        try:
            st.write("📡 Importando coletor...")
            import coletor_discricionarias as coletor

            st.write("📡 Baixando e processando siconv_convenio...")
            df = coletor.consolidar(forcar=False)

            if df is not None and len(df) > 0:
                status.update(label=f"✅ {len(df):,} registros coletados!", state="complete")
                st.cache_data.clear()
                st.rerun()
            else:
                status.update(label="⚠️ Coleta finalizada sem dados", state="error")
                st.warning("Nenhum dado retornado. Verifique os filtros no coletor.")

        except Exception as e:
            status.update(label="❌ Erro na coleta", state="error")
            st.error(f"**Erro:** {e}")
            import traceback
            st.code(traceback.format_exc(), language="python")


# ─── CARGA DE DADOS ───────────────────────────────────────────────────────────
@st.cache_data(show_spinner="⏳ Carregando Discricionárias...")
def load_discricionarias() -> pd.DataFrame:

    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()

    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        primeira_linha = f.readline()

    sep = ";" if ";" in primeira_linha else ("\t" if "\t" in primeira_linha else ",")

    df = pd.read_csv(CSV_PATH, sep=sep, encoding="utf-8-sig", low_memory=False)

    if df.empty:
        return df

    # Converte colunas numéricas
    for col in ["valor_global", "valor_repasse", "valor_contrapartida",
                "valor_empenhado", "valor_desembolsado"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Limpa colunas de ano
    for col in ["ano_proposta", "ano_assinatura"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(".0", "", regex=False)

    return df  # ← SEM filtro de UF aqui, já foi feito no coletor


# ─── RENDER PRINCIPAL ──────────────────────────────────────────────────────────
def render():
    """Renderiza a aba Discricionárias e Legais."""

    st.subheader("📂 Discricionárias e Legais — Transferegov")

    # ── Verifica se CSV existe ────────────────────────────────────────────
    if not os.path.exists(CSV_PATH):
        st.warning("⚠️ Dados ainda não coletados.")
        st.info("Clique no botão abaixo para coletar os dados do Transferegov "
                "(pode demorar alguns minutos).")
        if st.button("🚀 Coletar Dados Agora", type="primary"):
            executar_coletor()
        return

    # ── Carrega dados ─────────────────────────────────────────────────────
    df = load_discricionarias()

    if df.empty:
        st.error("❌ O CSV existe mas está vazio ou não foi lido corretamente.")
        st.info(f"📂 Caminho: `{CSV_PATH}`")
        if st.button("🚀 Re-coletar Dados", type="primary"):
            executar_coletor()
        return

    # ── Info + botão atualizar ────────────────────────────────────────────
    col_info, col_btn = st.columns([8, 2])
    with col_info:
        mtime = os.path.getmtime(CSV_PATH)
        dt    = datetime.datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M")
        st.caption(f"🕒 Última atualização: **{dt}** · {len(df):,} registros")
    with col_btn:
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            executar_coletor()

    # ── Filtros ───────────────────────────────────────────────────────────
    with st.expander("🔎 Filtros", expanded=True):
        c1, c2, c3, c4 = st.columns(4)

        anos   = (["Todos"] + sorted(df["ano_assinatura"].dropna().unique().tolist())
                  if "ano_assinatura" in df.columns else ["Todos"])
        sits   = (["Todas"] + sorted(df["situacao"].dropna().unique().tolist())
                  if "situacao" in df.columns else ["Todas"])
        orgaos = (["Todos"] + sorted(df["orgao_concedente"].dropna().unique().tolist())
                  if "orgao_concedente" in df.columns else ["Todos"])
        munis  = (["Todos"] + sorted(df["municipio_beneficiario"].dropna().unique().tolist())
                  if "municipio_beneficiario" in df.columns else ["Todos"])

        f_ano  = c1.selectbox("Ano Assinatura",   anos,   key="disc_ano")
        f_sit  = c2.selectbox("Situação",         sits,   key="disc_sit")
        f_org  = c3.selectbox("Órgão Concedente", orgaos, key="disc_org")
        f_muni = c4.selectbox("Município",        munis,  key="disc_muni")

    # ── Aplica filtros ────────────────────────────────────────────────────
    dff = df.copy()
    if f_ano  != "Todos" and "ano_assinatura"         in dff.columns:
        dff = dff[dff["ano_assinatura"] == f_ano]
    if f_sit  != "Todas" and "situacao"               in dff.columns:
        dff = dff[dff["situacao"] == f_sit]
    if f_org  != "Todos" and "orgao_concedente"       in dff.columns:
        dff = dff[dff["orgao_concedente"] == f_org]
    if f_muni != "Todos" and "municipio_beneficiario" in dff.columns:
        dff = dff[dff["municipio_beneficiario"] == f_muni]

    # ── KPIs ──────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total de Convênios",
              f"{len(dff):,}".replace(",", "."))
    k2.metric("💰 Valor Global",
              fmt_brl(dff["valor_global"].sum() if "valor_global" in dff.columns else 0))
    k3.metric("📥 Valor Repasse",
              fmt_brl(dff["valor_repasse"].sum() if "valor_repasse" in dff.columns else 0))
    k4.metric("💳 Valor Contrapartida",
              fmt_brl(dff["valor_contrapartida"].sum() if "valor_contrapartida" in dff.columns else 0))

    st.divider()

    # ── Gráficos — Linha 1 ───────────────────────────────────────────────
    g1, g2 = st.columns(2, gap="large")

    with g1:
        if "orgao_concedente" in dff.columns and "valor_repasse" in dff.columns:
            top_org = (
                dff.groupby("orgao_concedente")["valor_repasse"].sum()
                .reset_index().nlargest(10, "valor_repasse")
                .sort_values("valor_repasse")
            )
            top_org["label"] = top_org["valor_repasse"].apply(
                lambda x: f"R$ {x/1e6:.1f}M"
            )
            fig = px.bar(
                top_org, x="valor_repasse", y="orgao_concedente", orientation="h",
                title="🏦 Top 10 Órgãos Concedentes — Valor Repasse",
                color="valor_repasse", color_continuous_scale="Blues",
                labels={"valor_repasse": "Valor (R$)", "orgao_concedente": ""},
                text="label"
            )
            fig.update_layout(coloraxis_showscale=False, height=420,
                              margin=dict(l=10, r=80, t=50, b=10))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

    with g2:
        if "municipio_beneficiario" in dff.columns and "valor_repasse" in dff.columns:
            top_mun = (
                dff.groupby("municipio_beneficiario")["valor_repasse"].sum()
                .reset_index().nlargest(10, "valor_repasse")
                .sort_values("valor_repasse")
            )
            top_mun["label"] = top_mun["valor_repasse"].apply(
                lambda x: f"R$ {x/1e6:.1f}M"
            )
            fig = px.bar(
                top_mun, x="valor_repasse", y="municipio_beneficiario", orientation="h",
                title="🏙️ Top 10 Municípios — Valor Repasse",
                color="valor_repasse", color_continuous_scale="Greens",
                labels={"valor_repasse": "Valor (R$)", "municipio_beneficiario": ""},
                text="label"
            )
            fig.update_layout(coloraxis_showscale=False, height=420,
                              margin=dict(l=10, r=80, t=50, b=10))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

    # ── Gráficos — Linha 2 ───────────────────────────────────────────────
    g3, g4 = st.columns(2, gap="large")

    with g3:
        if "situacao" in dff.columns:
            sit = dff.groupby("situacao").size().reset_index(name="qtd")
            total_s = sit["qtd"].sum()
            sit["pct"] = sit["qtd"] / total_s * 100
            principais = sit[sit["pct"] >= 2].copy()
            outros_v   = sit[sit["pct"] < 2]["qtd"].sum()
            if outros_v > 0:
                principais = pd.concat([
                    principais,
                    pd.DataFrame([{
                        "situacao": "Outros",
                        "qtd": outros_v,
                        "pct": outros_v / total_s * 100
                    }])
                ], ignore_index=True)

            fig = go.Figure(go.Pie(
                labels=principais["situacao"], values=principais["qtd"],
                hole=0.45, textinfo="percent",
                hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>"
                              "%{percent}<extra></extra>",
                textposition="inside", insidetextorientation="radial",
                marker=dict(colors=px.colors.qualitative.Set2,
                            line=dict(color="white", width=2))
            ))
            fig.update_layout(
                title=dict(text="📌 Distribuição por Situação",
                           x=0, font=dict(size=15)),
                height=420,
                legend=dict(orientation="v", x=1.02, y=0.5,
                            yanchor="middle", font=dict(size=11)),
                margin=dict(l=10, r=180, t=50, b=10)
            )
            st.plotly_chart(fig, use_container_width=True)

    with g4:
        if "ano_assinatura" in dff.columns and "valor_repasse" in dff.columns:
            evo = (
                dff.groupby("ano_assinatura")
                .agg(valor=("valor_repasse", "sum"),
                     qtd=("valor_repasse", "count"))
                .reset_index().sort_values("ano_assinatura")
            )
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(x=evo["ano_assinatura"], y=evo["valor"],
                       name="Valor Repasse", marker_color="#1f77b4"),
                secondary_y=False
            )
            fig.add_trace(
                go.Scatter(x=evo["ano_assinatura"], y=evo["qtd"],
                           name="Qtd Convênios", mode="lines+markers",
                           line=dict(color="orange", width=2)),
                secondary_y=True
            )
            fig.update_layout(
                title="📅 Evolução por Ano", height=420,
                legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                margin=dict(l=10, r=10, t=50, b=80)
            )
            fig.update_yaxes(title_text="Valor (R$)", secondary_y=False)
            fig.update_yaxes(title_text="Quantidade",  secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)

    # ── Tabela + Download ─────────────────────────────────────────────────
    with st.expander("🔍 Dados detalhados"):
        colunas_tabela = [c for c in [
            "nr_convenio", "situacao", "municipio_beneficiario",
            "orgao_concedente", "orgao_superior", "modalidade",
            "valor_global", "valor_repasse", "valor_contrapartida",
            "valor_empenhado", "valor_desembolsado", "dt_assinatura"
        ] if c in dff.columns]

        st.dataframe(
            dff[colunas_tabela].sort_values("valor_repasse", ascending=False)
            if "valor_repasse" in dff.columns else dff[colunas_tabela],
            use_container_width=True,
            hide_index=True
        )

    st.download_button(
        label="📥 Baixar Relatório CSV",
        data=dff.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig"),
        file_name="discricionarias_to_filtrado.csv",
        mime="text/csv",
        type="primary"
    )
