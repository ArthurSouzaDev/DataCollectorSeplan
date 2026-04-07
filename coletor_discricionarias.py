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

# Filtros equivalentes ao MARCO.xlsx
FILTROS = {
    "uf":              "TO",
    "anos_proposta":   list(range(2019, 2027)),
    "anos_assinatura": list(range(2019, 2027)),
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

# Mapeamento das colunas brutas -> nomes padronizados (layout MARCO.xlsx)
# Ajuste apos rodar 'diagnosticar' para confirmar nomes reais
COLUNAS_SAIDA = {
    # convenio
    "nr_convenio":           "nr_convenio",
    "nr_proposta":           "nr_proposta",
    "dia_assin_conv":        "dt_assinatura",
    "dt_inicio_vigenc":      "dt_inicio_vigencia",
    "dt_fim_vigenc":         "dt_fim_vigencia",
    "sit_convenio":          "situacao",
    "objeto_convenio":       "objeto",
    "nm_munic_proponente":   "municipio_beneficiario",
    "uf_proponente":         "uf",
    "nm_proponente":         "proponente",
    "cnpj_proponente":       "cnpj_proponente",
    "nm_orgao_sup_conv":     "orgao_superior",
    "nm_orgao_conv":         "orgao_concedente",
    "vl_global_conv":        "valor_global",
    "vl_repasse_conv":       "valor_repasse",
    "vl_contrapartida_conv": "valor_contrapartida",
    "vl_empenhado_conv":     "valor_empenhado",
    "vl_desembolsado_conv":  "valor_desembolsado",
    "vl_saldo_reman_tesouro":"valor_saldo_tesouro",
    # proposta
    "ano_prop":              "ano_proposta",
    "modalidade_proposta":   "modalidade",
    "nm_programa":           "nome_programa",
    # emenda
    "nm_parlamentar":        "parlamentar",
    "tipo_parlamentar":      "tipo_parlamentar",
    "nr_emenda":             "nr_emenda",
    "valor_emenda_custeio":  "valor_custeio",
    "valor_emenda_investimento": "valor_investimento",
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
            caminho_csv, sep=";", encoding="latin-1",
            low_memory=False, on_bad_lines="skip"
        )

    url = f"{REPOSITORIO}/{nome_zip}"
    print(f"  [DOWNLOAD] {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=600, stream=True)
        resp.raise_for_status()

        conteudo  = b""
        total     = int(resp.headers.get("content-length", 0))
        baixado   = 0

        for chunk in resp.iter_content(chunk_size=131072):
            conteudo += chunk
            baixado  += len(chunk)
            if total:
                pct = baixado / total * 100
                print(f"\r  {pct:.0f}% — {baixado/1024/1024:.0f} MB", end="", flush=True)

        print(f"\r  [OK] {baixado/1024/1024:.0f} MB baixados        ")

        with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
            nome_interno = zf.namelist()[0]
            with zf.open(nome_interno) as f:
                dados_csv = f.read()

        with open(caminho_csv, "wb") as f:
            f.write(dados_csv)

        time.sleep(1)

        return pd.read_csv(
            io.BytesIO(dados_csv), sep=";", encoding="latin-1",
            low_memory=False, on_bad_lines="skip"
        )

    except requests.exceptions.HTTPError as e:
        print(f"\n  [ERRO HTTP {e.response.status_code}]")
        return None
    except requests.exceptions.Timeout:
        print(f"\n  [TIMEOUT] arquivo muito grande, tente novamente")
        return None
    except zipfile.BadZipFile:
        print(f"\n  [ERRO ZIP] arquivo corrompido")
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


def filtrar_uf(df: pd.DataFrame, label: str) -> pd.DataFrame:
    candidatas = [c for c in df.columns if "uf_propon" in c or c == "uf"]
    if not candidatas:
        candidatas = [c for c in df.columns if c.endswith("_uf") or c.startswith("uf_")]
    if candidatas:
        col   = candidatas[0]
        antes = len(df)
        df    = df[df[col].astype(str).str.strip().str.upper() == FILTROS["uf"]]
        print(f"  [FILTRO UF=TO via '{col}'] {antes:,} -> {len(df):,}")
    else:
        print(f"  [AVISO] {label}: coluna UF nao encontrada — {list(df.columns)[:10]}")
    return df


def converter_valores(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas vl_* de formato BR (1.234,56) para float."""
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
    """Aplica o mapeamento COLUNAS_SAIDA — ignora colunas inexistentes."""
    mapa = {k: v for k, v in COLUNAS_SAIDA.items() if k in df.columns}
    return df.rename(columns=mapa)


# --- PROCESSAMENTO ------------------------------------------------------------

def processar_convenio(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[CONVENIO]")
    df = baixar_e_extrair("convenio", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas")

    df = filtrar_uf(df, "convenio")

    # Extrai anos para filtro
    for col_data, col_ano in [
        ("dia_assin_conv",   "ano_assinatura"),
        ("dt_inicio_vigenc", "ano_inicio"),
    ]:
        df = extrair_ano(df, col_data, col_ano)

    # Filtra por ano de assinatura
    if "ano_assinatura" in df.columns:
        antes = len(df)
        df = df[df["ano_assinatura"].isin(FILTROS["anos_assinatura"])]
        print(f"  [FILTRO ANO ASSINATURA] {antes:,} -> {len(df):,}")

    df = converter_valores(df)
    return df


def processar_proposta(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[PROPOSTA]")
    df = baixar_e_extrair("proposta", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas")

    # Extrai ano da proposta
    df = extrair_ano(df, "dt_proposta", "ano_proposta_dt")

    # Coluna ano_prop pode ja existir como campo numerico
    if "ano_prop" not in df.columns and "ano_proposta_dt" in df.columns:
        df["ano_prop"] = df["ano_proposta_dt"]

    if "ano_prop" in df.columns:
        antes = len(df)
        df = df[df["ano_prop"].isin(FILTROS["anos_proposta"])]
        print(f"  [FILTRO ANO PROPOSTA] {antes:,} -> {len(df):,}")

    # Seleciona apenas colunas relevantes da proposta para o join
    colunas_prop = [
        c for c in df.columns
        if c in COLUNAS_SAIDA or c in [
            "nr_proposta", "modalidade_proposta", "nm_programa",
            "ano_prop", "tp_instrumento"
        ]
    ]
    return df[colunas_prop].copy()


def processar_emenda(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[EMENDA]")
    df = baixar_e_extrair("emenda", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas")

    # Seleciona colunas relevantes
    colunas_emenda = [
        c for c in df.columns
        if c in COLUNAS_SAIDA or c in [
            "nr_convenio", "nm_parlamentar", "tipo_parlamentar",
            "nr_emenda", "valor_emenda_custeio", "valor_emenda_investimento",
            "ano_emenda"
        ]
    ]
    df = df[colunas_emenda].copy()
    df = converter_valores(df)
    return df


# --- CONSOLIDACAO -------------------------------------------------------------

def consolidar(forcar: bool = False) -> pd.DataFrame | None:
    """
    Gera discricionarias_to.csv pronto para o app.py:
    - Filtrado por UF=TO e anos configurados
    - Colunas renomeadas para o layout do MARCO.xlsx
    - Valores numericos normalizados
    """
    df_conv   = processar_convenio(forcar)
    df_prop   = processar_proposta(forcar)
    df_emenda = processar_emenda(forcar)

    if df_conv is None:
        print("\n[FALHA] siconv_convenio indisponivel.")
        return None

    df_base = df_conv.copy()

    # Join com proposta
    if df_prop is not None:
        col_c = next((c for c in df_base.columns if c == "nr_proposta"), None)
        col_p = next((c for c in df_prop.columns if c == "nr_proposta"), None)
        if col_c and col_p:
            df_base = pd.merge(
                df_base, df_prop,
                on="nr_proposta", how="left",
                suffixes=("", "_prop")
            )
            print(f"\n[MERGE] convenio <-> proposta: {len(df_base):,}")

    # Join com emendas
    if df_emenda is not None:
        col_c = next((c for c in df_base.columns   if "nr_convenio" in c), None)
        col_e = next((c for c in df_emenda.columns if "nr_convenio" in c), None)
        if col_c and col_e:
            df_base = pd.merge(
                df_base, df_emenda,
                left_on=col_c, right_on=col_e,
                how="left", suffixes=("", "_emenda")
            )
            print(f"[MERGE] com emendas: {len(df_base):,}")

    # Renomeia para layout MARCO.xlsx
    df_base = renomear_colunas(df_base)

    # Garante colunas de ano presentes e como inteiro
    for col in ["ano_proposta", "ano_assinatura"]:
        if col in df_base.columns:
            df_base[col] = pd.to_numeric(df_base[col], errors="coerce").astype("Int64")

    # Remove colunas duplicadas geradas pelos merges
    df_base = df_base.loc[:, ~df_base.columns.duplicated()]

    # Salva
    saida = os.path.join(DATA_DIR, "discricionarias_to.csv")
    df_base.to_csv(saida, sep=";", index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print(f"Arquivo:         {saida}")
    print(f"Total linhas:    {len(df_base):,}")
    print(f"Total colunas:   {len(df_base.columns)}")
    print(f"Colunas:         {list(df_base.columns)}")
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
