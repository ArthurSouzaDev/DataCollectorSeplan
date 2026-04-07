# app_discricionarias.py
import os
import subprocess
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "data_discricionarias", "discricionarias_to.csv")


def executar_coletor():
    """Executa o coletor e exibe progresso no Streamlit."""
    with st.status("⏳ Coletando dados do Transferegov...", expanded=True) as status:
        st.write("📡 Baixando siconv_convenio...")
        result = subprocess.run(
            ["python", "coletor_discricionarias.py", "coletar"],
            capture_output=True, text=True, cwd=BASE_DIR
        )
        if result.returncode == 0:
            status.update(label="✅ Dados coletados com sucesso!", state="complete")
            st.rerun()
        else:
            status.update(label="❌ Erro na coleta", state="error")
            st.error(f"Erro:\n```\n{result.stderr[-2000:]}\n```")


@st.cache_data(show_spinner=False)
def load_discricionarias():
    df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8-sig", low_memory=False)

    # Garante colunas numéricas
    for col in ["valor_global", "valor_repasse", "valor_contrapartida",
                "valor_empenhado", "valor_desembolsado"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Ano como string para filtros
    for col in ["ano_proposta", "ano_assinatura"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(".0", "", regex=False)

    return df


def render():
    st.subheader("📂 Discricionárias e Legais — Transferegov")

    # ── Verifica se CSV existe ────────────────────────────────────────────────
    if not os.path.exists(CSV_PATH):
        st.warning("⚠️ Dados ainda não coletados.")
        st.info("Clique no botão abaixo para coletar os dados do Transferegov (pode demorar alguns minutos).")

        if st.button("🚀 Coletar Dados Agora", type="primary"):
            executar_coletor()
        return  # Para aqui até ter o CSV

    # ── Carrega dados ─────────────────────────────────────────────────────────
    df = load_discricionarias()

    # ── Botão para atualizar dados ────────────────────────────────────────────
    col_info, col_btn = st.columns([8, 2])
    with col_info:
        mtime = os.path.getmtime(CSV_PATH)
        import datetime
        dt = datetime.datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M")
        st.caption(f"🕒 Última atualização dos dados: **{dt}** · {len(df):,} registros")
    with col_btn:
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            executar_coletor()

    # ── Filtros ───────────────────────────────────────────────────────────────
    with st.expander("🔎 Filtros", expanded=True):
        c1, c2, c3, c4 = st.columns(4)

        anos = ["Todos"] + sorted(
            df["ano_assinatura"].dropna().unique().tolist()
        ) if "ano_assinatura" in df.columns else ["Todos"]

        sits = ["Todas"] + sorted(
            df["situacao"].dropna().unique().tolist()
        ) if "situacao" in df.columns else ["Todas"]

        orgaos = ["Todos"] + sorted(
            df["orgao_concedente"].dropna().unique().tolist()
        ) if "orgao_concedente" in df.columns else ["Todos"]

        munis = ["Todos"] + sorted(
            df["municipio_beneficiario"].dropna().unique().tolist()
        ) if "municipio_beneficiario" in df.columns else ["Todos"]

        f_ano  = c1.selectbox("Ano Assinatura",       anos,   key="disc_ano")
        f_sit  = c2.selectbox("Situação",             sits,   key="disc_sit")
        f_org  = c3.selectbox("Órgão Concedente",     orgaos, key="disc_org")
        f_muni = c4.selectbox("Município",            munis,  key="disc_muni")

    # ── Aplica filtros ────────────────────────────────────────────────────────
    dff = df.copy()
    if f_ano  != "Todos" and "ano_assinatura"        in dff.columns:
        dff = dff[dff["ano_assinatura"]        == f_ano]
    if f_sit  != "Todas" and "situacao"              in dff.columns:
        dff = dff[dff["situacao"]              == f_sit]
    if f_org  != "Todos" and "orgao_concedente"      in dff.columns:
        dff = dff[dff["orgao_concedente"]      == f_org]
    if f_muni != "Todos" and "municipio_beneficiario" in dff.columns:
        dff = dff[dff["municipio_beneficiario"] == f_muni]

    # ── KPIs ──────────────────────────────────────────────────────────────────
    def fmt_brl(v):
        return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total de Convênios",  f"{len(dff):,}".replace(",", "."))
    k2.metric("💰 Valor Global",        fmt_brl(dff["valor_global"].sum()        if "valor_global"        in dff.columns else 0))
    k3.metric("📥 Valor Repasse",       fmt_brl(dff["valor_repasse"].sum()       if "valor_repasse"       in dff.columns else 0))
    k4.metric("💳 Valor Contrapartida", fmt_brl(dff["valor_contrapartida"].sum() if "valor_contrapartida" in dff.columns else 0))

    st.divider()

    # ── Gráficos ──────────────────────────────────────────────────────────────
    g1, g2 = st.columns(2, gap="large")

    with g1:
        if "orgao_concedente" in dff.columns and "valor_repasse" in dff.columns:
            top_org = (
                dff.groupby("orgao_concedente")["valor_repasse"].sum()
                .reset_index()
                .nlargest(10, "valor_repasse")
                .sort_values("valor_repasse")
            )
            top_org["label"] = top_org["valor_repasse"].apply(lambda x: f"R$ {x/1e6:.1f}M")
            fig = px.bar(
                top_org, x="valor_repasse", y="orgao_concedente",
                orientation="h",
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
                .reset_index()
                .nlargest(10, "valor_repasse")
                .sort_values("valor_repasse")
            )
            top_mun["label"] = top_mun["valor_repasse"].apply(lambda x: f"R$ {x/1e6:.1f}M")
            fig = px.bar(
                top_mun, x="valor_repasse", y="municipio_beneficiario",
                orientation="h",
                title="🏙️ Top 10 Municípios — Valor Repasse",
                color="valor_repasse", color_continuous_scale="Greens",
                labels={"valor_repasse": "Valor (R$)", "municipio_beneficiario": ""},
                text="label"
            )
            fig.update_layout(coloraxis_showscale=False, height=420,
                              margin=dict(l=10, r=80, t=50, b=10))
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

    # Situação + Evolução
    g3, g4 = st.columns(2, gap="large")

    with g3:
        if "situacao" in dff.columns:
            sit = dff.groupby("situacao").size().reset_index(name="qtd")
            fig = go.Figure(go.Pie(
                labels=sit["situacao"], values=sit["qtd"],
                hole=0.45, textinfo="percent",
                marker=dict(colors=px.colors.qualitative.Set2,
                            line=dict(color="white", width=2))
            ))
            fig.update_layout(
                title="📌 Distribuição por Situação", height=420,
                legend=dict(orientation="v", x=1.02, y=0.5),
                margin=dict(l=10, r=180, t=50, b=10)
            )
            st.plotly_chart(fig, use_container_width=True)

    with g4:
        if "ano_assinatura" in dff.columns and "valor_repasse" in dff.columns:
            evo = (
                dff.groupby("ano_assinatura")
                .agg(valor=("valor_repasse", "sum"), qtd=("valor_repasse", "count"))
                .reset_index().sort_values("ano_assinatura")
            )
            from plotly.subplots import make_subplots
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=evo["ano_assinatura"], y=evo["valor"],
                                 name="Valor Repasse", marker_color="#1f77b4"),
                          secondary_y=False)
            fig.add_trace(go.Scatter(x=evo["ano_assinatura"], y=evo["qtd"],
                                     name="Qtd Convênios", mode="lines+markers",
                                     line=dict(color="orange", width=2)),
                          secondary_y=True)
            fig.update_layout(title="📅 Evolução por Ano", height=420,
                              legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                              margin=dict(l=10, r=10, t=50, b=80))
            st.plotly_chart(fig, use_container_width=True)

    # ── Tabela + Download ─────────────────────────────────────────────────────
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

        # ✅ Botão de download
        csv_bytes = dff[colunas_tabela].to_csv(
            index=False, sep=";", encoding="utf-8-sig"
        ).encode("utf-8-sig")

        st.download_button(
            label="📥 Baixar Relatório CSV",
            data=csv_bytes,
            file_name="discricionarias_to_filtrado.csv",
            mime="text/csv",
            type="primary"
        )
