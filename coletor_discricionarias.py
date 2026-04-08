# coletor_discricionarias.py
"""
Coleta e pre-filtragem dos dados de Discricionarias e Legais do Transferegov.
Fonte: http://repositorio.dados.gov.br/seges/detru/
Gera um CSV enxuto (apenas TO) para consumo pelo app.py
"""

import os
import io
import time
import zipfile
import requests
import pandas as pd

# --- CONFIGURACOES ------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "data_discricionarias")
CACHE_DIR  = os.path.join(DATA_DIR, "cache_bruto")
os.makedirs(DATA_DIR,  exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

REPOSITORIO = "http://repositorio.dados.gov.br/seges/detru"

ARQUIVOS = {
    "proposta":  "siconv_proposta.csv.zip",
    "convenio":  "siconv_convenio.csv.zip",
    "emenda":    "siconv_emenda.csv.zip",
    "programa":  "siconv_programa.csv.zip",
}

FILTROS = {
    "uf":              "TO",
    "anos_proposta":   list(range(2008, 2027)),
    "anos_assinatura": list(range(2008, 2027)),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Referer": (
        "https://www.gov.br/transferegov/pt-br/ferramentas-gestao/"
        "dados-abertos/download-dados"
    ),
}

COLUNAS_SAIDA = {
    "nr_convenio":                "nr_convenio",
    "id_proposta":                "id_proposta",
    "dia_assin_conv":             "dt_assinatura",
    "sit_convenio":               "situacao",
    "subsituacao_conv":           "subsituacao",
    "dia_inic_vigenc_conv":       "dt_inicio_vigencia",
    "dia_fim_vigenc_conv":        "dt_fim_vigencia",
    "vl_global_conv":             "valor_global",
    "vl_repasse_conv":            "valor_repasse",
    "vl_contrapartida_conv":      "valor_contrapartida",
    "vl_empenhado_conv":          "valor_empenhado",
    "vl_desembolsado_conv":       "valor_desembolsado",
    "vl_saldo_reman_tesouro":     "valor_saldo_tesouro",
    "nr_proposta":                "nr_proposta",
    "ano_prop":                   "ano_proposta",
    "modalidade_proposta":        "modalidade",
    "nm_programa":                "nome_programa",
    "uf_proponente":              "uf",
    "munic_proponente":           "municipio_beneficiario",
    "nm_munic_proponente":        "municipio_beneficiario",
    "nm_proponente":              "proponente",
    "cnpj_proponente":            "cnpj_proponente",
    "desc_orgao_sup":             "orgao_superior",
    "nm_orgao_sup_conv":          "orgao_superior",
    "desc_orgao":                 "orgao_concedente",
    "nm_orgao_conv":              "orgao_concedente",
    "nm_parlamentar":             "parlamentar",
    "tipo_parlamentar":           "tipo_parlamentar",
    "nr_emenda":                  "nr_emenda",
    "valor_emenda_custeio":       "valor_custeio",
    "valor_emenda_investimento":  "valor_investimento",
    "ano_emenda":                 "ano_emenda",
}


# --- UTILITARIOS --------------------------------------------------------------

def verificar_data_carga() -> str:
    url = f"{REPOSITORIO}/data_carga_siconv.txt"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        return f"indisponivel ({e})"


def baixar_e_extrair(chave: str, forcar: bool = False) -> pd.DataFrame | None:
    nome_zip = ARQUIVOS[chave]
    nome_csv = nome_zip.replace(".zip", "")
    caminho_csv = os.path.join(CACHE_DIR, nome_csv)

    if os.path.exists(caminho_csv) and not forcar:
        mb = os.path.getsize(caminho_csv) / 1024 / 1024
        print(f"  [CACHE] {nome_csv} ({mb:.0f} MB) — usando local")
        return pd.read_csv(
            caminho_csv, sep=";", encoding="utf-8-sig",
            low_memory=False, on_bad_lines="skip"
        )

    url = f"{REPOSITORIO}/{nome_zip}"
    print(f"  [DOWNLOAD] {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=600, stream=True)
        resp.raise_for_status()

        conteudo = b""
        total    = int(resp.headers.get("content-length", 0))
        baixado  = 0

        for chunk in resp.iter_content(chunk_size=131072):
            conteudo += chunk
            baixado  += len(chunk)
            if total:
                print(f"\r  {baixado/total*100:.0f}% — {baixado/1024/1024:.0f} MB",
                      end="", flush=True)

        print(f"\r  [OK] {baixado/1024/1024:.0f} MB baixados        ")

        with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
            nome_interno = zf.namelist()[0]
            with zf.open(nome_interno) as f:
                dados_csv = f.read()

        with open(caminho_csv, "wb") as f:
            f.write(dados_csv)

        time.sleep(1)

        return pd.read_csv(
            io.BytesIO(dados_csv), sep=";", encoding="utf-8-sig",
            low_memory=False, on_bad_lines="skip"
        )

    except requests.exceptions.HTTPError as e:
        print(f"\n  [ERRO HTTP {e.response.status_code}]")
        return None
    except requests.exceptions.Timeout:
        print(f"\n  [TIMEOUT]")
        return None
    except zipfile.BadZipFile:
        print(f"\n  [ERRO ZIP]")
        return None
    except Exception as e:
        print(f"\n  [ERRO] {e}")
        return None


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def extrair_ano(df: pd.DataFrame, col: str, nova_col: str) -> pd.DataFrame:
    if col in df.columns:
        df[nova_col] = pd.to_datetime(
            df[col], dayfirst=True, errors="coerce"
        ).dt.year
    return df


def filtrar_uf(df: pd.DataFrame, label: str,
               colunas_uf: list[str] | None = None) -> pd.DataFrame:
    """
    Filtra por UF usando apenas as colunas explicitamente informadas.
    Se colunas_uf=None, usa lista padrão SEM busca genérica por 'uf' no nome.
    """
    # ← FIX: removida busca genérica — evita pegar coluna errada
    if colunas_uf is None:
        colunas_uf = [
            "uf_proponente", "uf_propon",
            "uf_proponente_conv", "uf_munic_proponente"
        ]

    col = next((c for c in colunas_uf if c in df.columns), None)

    if col:
        antes = len(df)
        df = df[df[col].astype(str).str.strip().str.upper() == FILTROS["uf"]]
        print(f"  [FILTRO UF=TO via '{col}'] {antes:,} -> {len(df):,}")
    else:
        print(f"  [INFO] {label}: coluna UF nao encontrada — sem filtro UF")
        print(f"  Colunas disponíveis: {list(df.columns[:15])}")

    return df


def converter_valores(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col.startswith("vl_") or col.startswith("valor_"):
            df[col] = (
                df[col].astype(str).str.strip()
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
                .fillna(0.0)
            )
    return df


def renomear_colunas(df: pd.DataFrame) -> pd.DataFrame:
    mapa = {k: v for k, v in COLUNAS_SAIDA.items() if k in df.columns}
    return df.rename(columns=mapa)


# --- PROCESSAMENTO ------------------------------------------------------------

def processar_convenio(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[CONVENIO]")
    df = baixar_e_extrair("convenio", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas | Colunas: {list(df.columns[:10])}")

    # ← FIX: convenio NÃO filtra por UF — UF só existe na proposta
    # filtrar_uf removido daqui para evitar zerar o DataFrame

    df = extrair_ano(df, "dia_assin_conv",   "ano_assinatura")
    df = extrair_ano(df, "dia_inic_vigenc_conv", "ano_inicio")

    # ← FIX: filtro de ano só se a coluna existir e tiver dados
    if "ano_assinatura" in df.columns:
        antes = len(df)
        df_filtrado = df[df["ano_assinatura"].isin(FILTROS["anos_assinatura"])]
        # ← FIX: se filtro zerar, mantém original com aviso
        if len(df_filtrado) == 0:
            print(f"  [AVISO] Filtro ano zerou convenios — mantendo todos ({antes:,})")
        else:
            df = df_filtrado
            print(f"  [FILTRO ANO ASSINATURA] {antes:,} -> {len(df):,}")

    df = converter_valores(df)
    print(f"  Convenios mantidos: {len(df):,}")
    return df


def processar_proposta(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[PROPOSTA]")
    df = baixar_e_extrair("proposta", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas | Colunas: {list(df.columns[:10])}")

    # UF está na proposta — filtra TO com colunas explícitas
    df = filtrar_uf(df, "proposta", colunas_uf=["uf_proponente", "uf_propon"])

    # ← FIX: se saiu vazio, mostra valores reais de UF para diagnóstico
    if len(df) == 0:
        df_raw = baixar_e_extrair("proposta", forcar=False)
        if df_raw is not None:
            df_raw = normalizar_colunas(df_raw)
            col_uf = next((c for c in df_raw.columns if "uf" in c.lower()), None)
            if col_uf:
                print(f"  [DEBUG] Valores únicos em '{col_uf}': "
                      f"{df_raw[col_uf].unique()[:20]}")
        return pd.DataFrame()

    df = extrair_ano(df, "dt_proposta", "ano_proposta_dt")

    if "ano_prop" not in df.columns and "ano_proposta_dt" in df.columns:
        df["ano_prop"] = df["ano_proposta_dt"]

    if "ano_prop" in df.columns:
        antes = len(df)
        df_filtrado = df[df["ano_prop"].isin(FILTROS["anos_proposta"])]
        if len(df_filtrado) == 0:
            print(f"  [AVISO] Filtro ano zerou propostas — mantendo todas ({antes:,})")
        else:
            df = df_filtrado
            print(f"  [FILTRO ANO PROPOSTA] {antes:,} -> {len(df):,}")

    print(f"  Propostas TO mantidas: {len(df):,}")
    return df.copy()


def processar_emenda(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[EMENDA]")
    df = baixar_e_extrair("emenda", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas")

    COLUNAS_EMENDA_DESEJADAS = [
        "id_proposta",
        "nr_emenda",
        "nome_parlamentar",
        "tipo_parlamentar",
        "ind_impositivo",
        "beneficiario_emenda",
        "cod_programa_emenda",
        "valor_repasse_proposta_emenda",
        "valor_repasse_emenda",
    ]

    colunas_emenda   = [c for c in COLUNAS_EMENDA_DESEJADAS if c in df.columns]
    colunas_faltando = [c for c in COLUNAS_EMENDA_DESEJADAS if c not in df.columns]

    if colunas_faltando:
        print(f"  [AVISO] Colunas nao encontradas: {colunas_faltando}")

    df = df[colunas_emenda].copy()
    df = converter_valores(df)

    print(f"  Colunas selecionadas: {colunas_emenda}")
    return df


# --- CONSOLIDACAO -------------------------------------------------------------

def consolidar(forcar: bool = False) -> pd.DataFrame | None:
    df_conv   = processar_convenio(forcar)
    df_prop   = processar_proposta(forcar)
    df_emenda = processar_emenda(forcar)

    # ← FIX: validação explícita com mensagem clara
    if df_conv is None:
        print("\n[FALHA] siconv_convenio indisponivel.")
        return None

    if df_prop is None or len(df_prop) == 0:
        print("\n[FALHA] siconv_proposta indisponivel ou vazio após filtros.")
        print("  → Verifique se a coluna 'uf_proponente' existe no arquivo.")
        return None

    df_base = df_prop.copy()
    print(f"\n[BASE] Propostas TO: {len(df_base):,}")

    # ── Join proposta → convenio ──────────────────────────────────────────
    col_prop_id = next((c for c in df_base.columns
                        if c in ["id_proposta", "nr_proposta"]), None)
    col_conv_id = next((c for c in df_conv.columns
                        if c in ["id_proposta", "nr_proposta"]), None)

    print(f"  [JOIN] Chave proposta: '{col_prop_id}' | Chave convenio: '{col_conv_id}'")

    if col_prop_id and col_conv_id:
        antes = len(df_base)
        df_base = pd.merge(
            df_base, df_conv,
            left_on=col_prop_id, right_on=col_conv_id,
            how="left", suffixes=("", "_conv")
        )
        df_base = df_base.loc[:, ~df_base.columns.duplicated()]
        print(f"  [MERGE proposta<->convenio] {antes:,} -> {len(df_base):,} linhas")

        if len(df_base) > antes * 1.05:
            print("  ⚠️  MERGE multiplicou linhas! "
                  "Deduplica convenio por id_proposta.")
            # ← FIX: deduplica convenio antes do merge para evitar explosão
            df_conv_dedup = df_conv.drop_duplicates(subset=[col_conv_id])
            df_base = pd.merge(
                df_prop, df_conv_dedup,
                left_on=col_prop_id, right_on=col_conv_id,
                how="left", suffixes=("", "_conv")
            )
            df_base = df_base.loc[:, ~df_base.columns.duplicated()]
            print(f"  [MERGE dedup] -> {len(df_base):,} linhas")
    else:
        print(f"  [AVISO] Chave id_proposta não encontrada!")
        print(f"  Colunas proposta:  {list(df_prop.columns[:10])}")
        print(f"  Colunas convenio:  {list(df_conv.columns[:10])}")

    # ── Join com emendas ──────────────────────────────────────────────────
    if df_emenda is not None and len(df_emenda) > 0:
        col_base  = next((c for c in df_base.columns   if c == "id_proposta"), None)
        col_emend = next((c for c in df_emenda.columns if c == "id_proposta"), None)

        if col_base and col_emend:
            agg_dict = {}
            for campo, agg in [
                ("nr_emenda",                    lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("nome_parlamentar",             lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("tipo_parlamentar",             lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("ind_impositivo",               lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("beneficiario_emenda",          lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("cod_programa_emenda",          lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("valor_repasse_proposta_emenda","sum"),
                ("valor_repasse_emenda",         "sum"),
            ]:
                if campo in df_emenda.columns:
                    agg_dict[campo] = agg

            df_emenda_agg = df_emenda.groupby(col_emend, as_index=False).agg(agg_dict)
            antes = len(df_base)
            df_base = pd.merge(
                df_base, df_emenda_agg,
                left_on=col_base, right_on=col_emend,
                how="left", suffixes=("", "_emenda")
            )
            df_base = df_base.loc[:, ~df_base.columns.duplicated()]
            com_emenda = df_base["nr_emenda"].notna().sum() \
                         if "nr_emenda" in df_base.columns else 0
            print(f"  [MERGE emendas] {antes:,} -> {len(df_base):,} | "
                  f"Com emenda: {com_emenda:,}")

    # ── Finaliza ──────────────────────────────────────────────────────────
    df_base = renomear_colunas(df_base)

    for col in ["ano_proposta", "ano_assinatura"]:
        if col in df_base.columns:
            df_base[col] = pd.to_numeric(df_base[col], errors="coerce").astype("Int64")

    df_base = df_base.loc[:, ~df_base.columns.duplicated()]

    # ← FIX: validação final antes de salvar
    if len(df_base) == 0:
        print("\n[FALHA] DataFrame final vazio — CSV não será salvo.")
        return None

    saida = os.path.join(DATA_DIR, "discricionarias_to.csv")
    df_base.to_csv(saida, sep=";", index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print(f"Arquivo:         {saida}")
    print(f"Total linhas:    {len(df_base):,}")
    print(f"Total colunas:   {len(df_base.columns)}")
    print("=" * 60)

    return df_base


# --- DIAGNOSTICO --------------------------------------------------------------

def diagnosticar():
    print(f"\nRepositorio:         {REPOSITORIO}")
    print(f"Ultima atualizacao:  {verificar_data_carga()}\n")

    for chave in ARQUIVOS:
        df = baixar_e_extrair(chave, forcar=False)
        if df is not None:
            df = normalizar_colunas(df)
            print(f"\n[{chave.upper()}] {ARQUIVOS[chave]}")
            print(f"  Linhas:  {len(df):,}")
            print(f"  Colunas: {list(df.columns)}\n")


# --- ENTRY POINT --------------------------------------------------------------

if __name__ == "__main__":
    import sys

    modo = sys.argv[1] if len(sys.argv) > 1 else "coletar"

    print("\n" + "=" * 60)
    print("  Coletor Discricionarias e Legais — Transferegov")
    print(f"  Repositorio:         {REPOSITORIO}")
    print(f"  Ultima atualizacao:  {verificar_data_carga()}")
    print("=" * 60 + "\n")

    if modo == "diagnosticar":
        diagnosticar()
    elif modo == "forcar":
        print("Coleta FORCADA — re-download de tudo...")
        consolidar(forcar=True)
    else:
        print("Coleta com cache local...")
        consolidar(forcar=False)
