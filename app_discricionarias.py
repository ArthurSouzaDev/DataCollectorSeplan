# app_discricionarias.py
import os
import sys
import importlib
import datetime
from contextlib import nullcontext
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CSV_PATH  = os.path.join(BASE_DIR, "data_discricionarias", "discricionarias_to.csv")

ALIASES_COLUNAS = {
    "munic_proponente": "municipio_beneficiario",
    "desc_orgao_sup": "orgao_superior",
    "desc_orgao": "orgao_concedente",
}

COLUNAS_ESSENCIAIS = [
    "situacao",
    "municipio_beneficiario",
    "orgao_concedente",
    "valor_global",
    "valor_repasse",
    "natureza_juridica",
    ""
]


# ─── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_brl(v: float) -> str:
    return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


def harmonizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Ajusta nomes antigos/brutos para os nomes esperados pela interface."""
    renomear = {}
    for origem, destino in ALIASES_COLUNAS.items():
        if origem in df.columns and destino not in df.columns:
            renomear[origem] = destino
        elif origem in df.columns and destino in df.columns:
            # ← NOVO: destino já existe, dropa a duplicata em vez de sobrescrever
            print(f"  [HARMONIZAR] '{destino}' já existe — descartando '{origem}'")
            df = df.drop(columns=[origem])
    if renomear:
        df = df.rename(columns=renomear)
    return df


def colunas_ausentes(df: pd.DataFrame) -> list[str]:
    """Retorna colunas mínimas esperadas que não vieram no CSV."""
    return [col for col in COLUNAS_ESSENCIAIS if col not in df.columns]


def atualizar_status(status, **kwargs):
    """Atualiza o status apenas quando o objeto existe."""
    if status is not None and hasattr(status, "update"):
        status.update(**kwargs)


def _diagnostico_cache():
    """Exibe diagnóstico dos arquivos de cache no status atual."""
    st.write("🔎 Verificando arquivos em cache...")
    cache_dir = os.path.join(BASE_DIR, "data_discricionarias", "cache_bruto")
    for nome in ["siconv_convenio.csv", "siconv_proposta.csv", "siconv_emenda.csv"]:
        caminho = os.path.join(cache_dir, nome)
        if os.path.exists(caminho):
            mb = os.path.getsize(caminho) / 1024 / 1024
            st.write(f"  ✅ {nome} — {mb:.0f} MB")
        else:
            st.write(f"  ⬇️ {nome} — será baixado")


def _importar_coletor():
    """Garante reimport limpo do módulo coletor_discricionarias."""
    if BASE_DIR not in sys.path:
        sys.path.insert(0, BASE_DIR)

    # Remove versão antiga do cache de módulos para forçar reimport
    if "coletor_discricionarias" in sys.modules:
        del sys.modules["coletor_discricionarias"]

    import coletor_discricionarias as coletor
    importlib.reload(coletor)
    return coletor

def executar_coletor(forcar: bool = False):
    """Importa e executa o coletor diretamente no mesmo processo Python."""
    label_inicial = (
        "⏳ Forçando re-download dos dados..." if forcar
        else "⏳ Coletando dados do Transferegov..."
    )

    status_ctx = st.status(label_inicial, expanded=True)
    if status_ctx is None:
        status_ctx = nullcontext(None)

    with status_ctx as status:
        try:
            st.write("📡 Importando coletor...")
            coletor = _importar_coletor()

            _diagnostico_cache()

            st.write("⚙️ Consolidando dados TO...")
            df = coletor.consolidar(forcar=forcar)

            if df is None:
                atualizar_status(status, label="❌ Coletor não retornou dados", state="error")
                st.error("O coletor falhou em alguma etapa e retornou `None`.")
                st.info("Revise as mensagens de CONVENIO, PROPOSTA e EMENDA exibidas acima.")
                return pd.DataFrame()

            if not df.empty:
                atualizar_status(
                    status,
                    label=f"✅ {len(df):,} registros coletados!",
                    state="complete"
                )

                if os.path.exists(CSV_PATH):
                    st.write(f"✅ CSV final encontrado em: {CSV_PATH}")
                else:
                    st.warning(f"⚠️ O DataFrame foi gerado, mas o CSV não foi encontrado em: {CSV_PATH}")

                st.cache_data.clear()
                return df

            atualizar_status(status, label="⚠️ Coleta finalizada sem dados", state="error")
            st.warning("O coletor executou, mas retornou um DataFrame vazio.")
            st.info("Isso normalmente indica filtro excessivo ou etapa anterior zerada.")
            return pd.DataFrame()

        except Exception as e:
            atualizar_status(status, label="❌ Erro na coleta", state="error")
            st.error(f"**Erro:** {e}")
            import traceback
            st.code(traceback.format_exc(), language="python")
            return pd.DataFrame()

# ─── CARGA DE DADOS ───────────────────────────────────────────────────────────
@st.cache_data(show_spinner="⏳ Carregando Discricionárias...")
def load_discricionarias() -> pd.DataFrame:

    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()

    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        primeira_linha = f.readline()

    sep = ";" if ";" in primeira_linha else ("\t" if "\t" in primeira_linha else ",")

    df = pd.read_csv(CSV_PATH, sep=sep, encoding="utf-8-sig", low_memory=False)
    df = harmonizar_colunas(df)

    if df.empty:
        st.warning("O CSV foi encontrado, mas está vazio.")
        return df

    for col in ["valor_global", "valor_repasse", "valor_contrapartida",
                "valor_empenhado", "valor_desembolsado"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in ["ano_proposta", "ano_assinatura"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(".0", "", regex=False)

    # ← ADICIONAR: fallback se coluna não veio no CSV
    if "natureza_juridica" not in df.columns:
        df["natureza_juridica"] = "Não informado"

    # Otimiza memória com category
    for col in ["situacao", "orgao_concedente", "municipio_beneficiario",
                "natureza_juridica"]:         
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df



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
            executar_coletor(forcar=False)
        return

    # ── Carrega dados ─────────────────────────────────────────────────────
    df = load_discricionarias()

    if df.empty:
        st.error("❌ O CSV existe mas está vazio ou não foi lido corretamente.")
        st.info(f"📂 Caminho: `{CSV_PATH}`")
        if st.button("🚀 Re-coletar Dados", type="primary"):
            executar_coletor(forcar=False)
        return

    faltantes = colunas_ausentes(df)
    if faltantes:
        st.error("❌ O CSV foi carregado, mas está com esquema incompatível para a aba.")
        st.warning("Colunas ausentes: " + ", ".join(faltantes))
        st.info(f"📂 Caminho: `{CSV_PATH}`")
        if st.button("🔄 Reprocessar Dados", type="primary"):
            executar_coletor(forcar=False)
        return

    # ── Info + botões atualizar / forçar re-download ───────────────────────
    col_info, col_btn, col_force = st.columns([6, 2, 2])
    with col_info:
        mtime = os.path.getmtime(CSV_PATH)
        dt    = datetime.datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M")
        st.caption(f"🕒 Última atualização: **{dt}** · {len(df):,} registros")
    with col_btn:
        if st.button("🔄 Atualizar Dados"):
            df_novo = executar_coletor()
            if not df_novo.empty:
                st.rerun()
    with col_force:
        if st.button("⚠️ Forçar Re-download", use_container_width=True):
            st.cache_data.clear()
            executar_coletor(forcar=True)

    # ── Filtros ───────────────────────────────────────────────────────────

    with st.expander("🔎 Filtros", expanded=True):
        c1, c2, c3, c4, c5 = st.columns(5)

        anos_ass  = (sorted(pd.to_numeric(df["ano_assinatura"], errors="coerce")
                            .dropna().astype(int).unique().tolist())
                    if "ano_assinatura" in df.columns else [])

        anos_prop = (sorted(pd.to_numeric(df["ano_proposta"], errors="coerce")
                            .dropna().astype(int).unique().tolist())
                    if "ano_proposta" in df.columns else [])
        sits      = (["Todas"] + sorted(df["situacao"].dropna().unique().tolist())
                    if "situacao" in df.columns else ["Todas"])
        orgaos    = (["Todos"] + sorted(df["orgao_concedente"].dropna().unique().tolist())
                    if "orgao_concedente" in df.columns else ["Todos"])
        nats      = (["Todas"] + sorted(df["natureza_juridica"].dropna().unique().tolist())
                    if "natureza_juridica" in df.columns else ["Todas"])

        f_ano_ass  = c1.multiselect("Ano Assinatura",  anos_ass,  placeholder="Todos", key="disc_ano_ass")
        f_ano_prop = c2.multiselect("Ano Proposta",    anos_prop, placeholder="Todos", key="disc_ano_prop")
        f_sit      = c3.selectbox("Situação",          sits,      key="disc_sit")
        f_org      = c4.selectbox("Órgão Concedente",  orgaos,    key="disc_org")
        f_nat      = c5.selectbox("Natureza jurídica",        nats,      key="disc_nat")

    # ── Aplicação ───────────────────────────────────────────────────────────

    dff = df.copy()
    if f_ano_ass and "ano_assinatura" in dff.columns:
        dff = dff[pd.to_numeric(dff["ano_assinatura"], errors="coerce").isin(f_ano_ass)]
    if f_ano_prop and "ano_proposta" in dff.columns:
        dff = dff[pd.to_numeric(dff["ano_proposta"], errors="coerce").isin(f_ano_prop)]
    if f_sit != "Todas" and "situacao" in dff.columns:
        dff = dff[dff["situacao"] == f_sit]
    if f_org != "Todos" and "orgao_concedente" in dff.columns:
        dff = dff[dff["orgao_concedente"] == f_org]
    if f_nat != "Todas" and "natureza_juridica" in dff.columns:
        dff = dff[dff["natureza_juridica"] == f_nat]

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
# ── Gráficos — Linha 2 ───────────────────────────────────────────
    g3, g4, g5 = st.columns(3, gap="large")   # ← era 2, agora 3

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

# ── NOVO: Gráfico Natureza Jurídica ──────────────────────────────
    with g5:
        if "natureza_juridica" in dff.columns:
            nat = (dff.groupby("natureza_juridica", observed=True)
                .size().reset_index(name="qtd")
                .sort_values("qtd", ascending=False))
            total_n = nat["qtd"].sum()
            nat["pct"] = nat["qtd"] / total_n * 100
            principais_n = nat[nat["pct"] >= 2].copy()
            outros_n = nat[nat["pct"] < 2]["qtd"].sum()
            if outros_n > 0:
                principais_n = pd.concat([
                    principais_n,
                    pd.DataFrame([{
                        "natureza_juridica": "Outros",
                        "qtd": outros_n,
                        "pct": outros_n / total_n * 100
                    }])
                ], ignore_index=True)

            fig = go.Figure(go.Pie(
                labels=principais_n["natureza_juridica"],
                values=principais_n["qtd"],
                hole=0.45, textinfo="percent",
                hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>"
                            "%{percent}<extra></extra>",
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


    # ── Tabela + Download ─────────────────────────────────────────────────
    with st.expander("🔍 Dados detalhados"):
        colunas_tabela = [c for c in [
            "nr_convenio", "situacao", "municipio_beneficiario",
            "orgao_concedente", "orgao_superior", "modalidade",
            "natureza_juridica",          
            "valor_global", "valor_repasse", "valor_contrapartida",
            "valor_empenhado", "valor_desembolsado", "dt_assinatura",

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
