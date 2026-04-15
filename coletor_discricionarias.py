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
URL_PAGAMENTO = "https://repositorio.dados.gov.br/seges/detru/pagamento.csv"
CACHE_PAGAMENTO = os.path.join(CACHE_DIR, "pagamento.csv")

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
    # ── Convênio ──────────────────────────────────────────────────────────────────
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

    # ── NOVOS — necessários para KPIs corretos ────────────────────────────────────
    "vl_ingresso_contrapartida":  "vl_ingresso_contrapartida",   # ← Valor Liberado
    "vl_rendimento_aplicacao":    "vl_rendimento_aplicacao",     # ← Valor Liberado
    "vl_saldo_reman_convenente":  "vl_saldo_reman_convenente",   # ← Valores Devolvidos

    # ── Proposta ──────────────────────────────────────────────────────────────────
    "nr_proposta":                "nr_proposta",
    "ano_prop":                   "ano_proposta",
    "modalidade_proposta":        "modalidade",
    "nm_programa":                "nome_programa",

    # ── Proponente / Município ────────────────────────────────────────────────────
    "nm_munic_proponente":        "municipio_beneficiario",
    "munic_proponente":           "municipio_beneficiario",
    "nm_proponente":              "proponente",
    "cnpj_proponente":            "cnpj_proponente",
    "natureza_juridica":          "natureza_juridica",
    "uf_proponente":              "uf",

    # ── Órgão ─────────────────────────────────────────────────────────────────────
    "nm_orgao_sup_conv":          "orgao_superior",
    "desc_orgao_sup":             "orgao_superior",
    "nm_orgao_conv":              "orgao_concedente",
    "desc_orgao":                 "orgao_concedente",

    # ── Emenda ────────────────────────────────────────────────────────────────────
    "nm_parlamentar":             "parlamentar",
    "tipo_parlamentar":           "tipo_parlamentar",
    "nr_emenda":                  "nr_emenda",
    "valor_emenda_custeio":       "valor_custeio",
    "valor_emenda_investimento":  "valor_investimento",
    "ano_emenda":                 "ano_emenda",

    # ── Financeiro ────────────────────────────────────────────────────────────────
    "vl_saldo_conta":             "valor_saldo_conta",
    "valor_pago":                 "valor_pago",
}




# --- UTILITARIOS --------------------------------------------------------------
def _detectar_sep(caminho_ou_conteudo: str) -> str:
    """Detecta separador lendo a primeira linha."""
    linha = caminho_ou_conteudo.split("\n")[0] if "\n" in caminho_ou_conteudo \
            else open(caminho_ou_conteudo, encoding="utf-8-sig").readline()
    return ";" if ";" in linha else ("\t" if "\t" in linha else ",")

def baixar_pagamento(forcar: bool = False) -> pd.DataFrame:
    """Baixa pagamento.csv e retorna agregado por convênio."""
    if not forcar and os.path.exists(CACHE_PAGAMENTO):
        print("[PAGAMENTO] Usando cache...")
        sep = _detectar_sep(CACHE_PAGAMENTO)
        df  = pd.read_csv(CACHE_PAGAMENTO, sep=sep, encoding="utf-8-sig", low_memory=False)
    else:
        print("[PAGAMENTO] Baixando...")
        resp = requests.get(URL_PAGAMENTO, headers=HEADERS, timeout=300)
        resp.raise_for_status()
        conteudo = resp.content.decode("utf-8-sig")
        sep = _detectar_sep(conteudo)
        df  = pd.read_csv(io.StringIO(conteudo), sep=sep, low_memory=False)
        df.to_csv(CACHE_PAGAMENTO, index=False, sep=";", encoding="utf-8-sig")

    # Normaliza nomes de colunas
    df.columns = df.columns.str.strip().str.lower()

    # Identifica colunas com tolerância de nome
    col_vl = next(
        (c for c in df.columns if c in ["vl_pago", "valor_pago", "vl_valor_pago"]),
        None
    )
    col_nr = next(
        (c for c in df.columns if c in ["nr_convenio", "nr_conv"]),
        None
    )

    if col_vl is None or col_nr is None:
        print(f"  [PAGAMENTO] Colunas disponíveis: {df.columns.tolist()}")
        print("  [PAGAMENTO] ⚠️ Colunas esperadas não encontradas — valor_pago zerado.")
        return pd.DataFrame(columns=["nr_convenio", "valor_pago"])

    agg = (
        df.groupby(col_nr)[col_vl]
        .sum()
        .reset_index()
        .rename(columns={col_nr: "nr_convenio", col_vl: "valor_pago"})
    )
    agg["nr_convenio"] = agg["nr_convenio"].astype(str).str.strip()
    print(f"  [PAGAMENTO] {len(agg):,} convênios com pagamento agregado.")
    return agg   # ← return obrigatório

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


def _detectar_coluna_uf(df: pd.DataFrame, label: str) -> str | None:
    """
    Detecta a coluna de UF com segurança:
    1. Tenta lista de nomes conhecidos (prioridade)
    2. Inspeciona valores reais para confirmar que é coluna de UF brasileira
    """
    CANDIDATAS_CONHECIDAS = [
        "uf_proponente", "uf_propon",
        "uf_proponente_conv", "uf_munic_proponente",
        "uf_convenente", "uf_beneficiario",
    ]

    # Prioridade 1: nomes conhecidos
    for c in CANDIDATAS_CONHECIDAS:
        if c in df.columns:
            print(f"  [UF] Coluna detectada por nome: '{c}'")
            return c

    # Prioridade 2: busca heurística por valores reais (siglas de UF)
    UFS_BRASILEIRAS = {
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
        "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
        "RS","RO","RR","SC","SP","SE","TO"
    }
    for c in df.columns:
        if "uf" in c.lower():
            amostra = set(df[c].dropna().astype(str).str.strip().str.upper().unique()[:30])
            if amostra and amostra.issubset(UFS_BRASILEIRAS):
                print(f"  [UF] Coluna detectada por heurística: '{c}' | valores: {amostra}")
                return c

    print(f"  [AVISO] {label}: nenhuma coluna de UF encontrada.")
    print(f"  Colunas disponíveis: {list(df.columns[:20])}")
    return None


def filtrar_uf(df: pd.DataFrame, label: str,
               colunas_uf: list[str] | None = None) -> pd.DataFrame:
    """
    Filtra por UF=TO com detecção robusta de coluna.
    Nunca retorna vazio silenciosamente — sempre loga o motivo.
    """
    col = None

    # Se colunas_uf foi passado explicitamente, tenta primeiro
    if colunas_uf:
        col = next((c for c in colunas_uf if c in df.columns), None)
        if col:
            print(f"  [UF] Usando coluna explícita: '{col}'")

    # Fallback: detecção automática
    if col is None:
        col = _detectar_coluna_uf(df, label)

    if col is None:
        print(f"  [INFO] {label}: sem filtro UF aplicado — coluna não encontrada.")
        return df

    # Diagnóstico antes do filtro
    valores_unicos = df[col].dropna().astype(str).str.strip().str.upper().unique()
    tem_to = "TO" in valores_unicos
    print(f"  [UF DEBUG] Coluna='{col}' | Únicos (amostra): {list(valores_unicos[:10])}")
    print(f"  [UF DEBUG] Contém 'TO': {tem_to} | Total antes: {len(df):,}")

    if not tem_to:
        print(f"  [CRÍTICO] UF='TO' não encontrado na coluna '{col}'!")
        print(f"  Todos os valores: {list(valores_unicos)}")
        # ← Retorna o df original em vez de zerar — evita falha silenciosa
        return df

    antes = len(df)
    df = df[df[col].astype(str).str.strip().str.upper() == FILTROS["uf"]]
    print(f"  [FILTRO UF=TO via '{col}'] {antes:,} → {len(df):,}")
    return df

def converter_valores(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if not (col.startswith("vl_") or col.startswith("valor_")):
            continue

        amostra = df[col].dropna().astype(str).str.strip().head(100)

        # Conta vírgulas e pontos para determinar o formato real
        n_virgula = amostra.str.count(",").sum()
        n_ponto   = amostra.str.count(r"\.").sum()

        tem_virgula     = n_virgula > 0
        tem_ponto       = n_ponto > 0
        multiplos_pontos = amostra.str.count(r"\.").max() > 1  # ex: 32.571.009

        serie = df[col].astype(str).str.strip()

        if tem_virgula and tem_ponto:
            # Formato BR claro: 1.234,56 → remove ponto, troca vírgula por ponto
            serie = serie.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

        elif tem_virgula and not tem_ponto:
            # Só vírgula: 1234,56 → troca vírgula por ponto
            serie = serie.str.replace(",", ".", regex=False)

        elif not tem_virgula and multiplos_pontos:

            serie = serie.str.replace(".", "", regex=False)

        # else: formato internacional sem separador → não mexe

        df[col] = pd.to_numeric(serie, errors="coerce").fillna(0.0)

        # ── Proteção pós-conversão: saldo não pode exceder repasse ────────────
        if col == "vl_saldo_conta" and "vl_repasse_conv" in df.columns:
            repasse = pd.to_numeric(df["vl_repasse_conv"], errors="coerce").fillna(0.0)
            mascara_invalida = df[col] > repasse * 1.1  # tolerância de 10%
            if mascara_invalida.sum() > 0:
                print(f"  [SALDO CONTA] ⚠️ {mascara_invalida.sum()} registros com saldo > repasse "
                      f"— dividindo por 100 (conversão centavos→reais)")
                df.loc[mascara_invalida, col] = df.loc[mascara_invalida, col] / 100

    return df

def renomear_colunas(df: pd.DataFrame) -> pd.DataFrame:
    mapa = {}
    for origem, destino in COLUNAS_SAIDA.items():
        if origem in df.columns:
            # Evita sobrescrever coluna destino já mapeada por outra origem
            if destino not in mapa.values():
                mapa[origem] = destino
            else:
                print(f"  [AVISO] Coluna destino '{destino}' já mapeada — ignorando '{origem}'")
    return df.rename(columns=mapa)
# --- PROCESSAMENTO ------------------------------------------------------------
def processar_convenio(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[CONVENIO]")
    df = baixar_e_extrair("convenio", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas | Colunas: {list(df.columns[:10])}")

    df = extrair_ano(df, "dia_assin_conv",      "ano_assinatura")
    df = extrair_ano(df, "dia_inic_vigenc_conv", "ano_inicio")

    if "ano_assinatura" in df.columns:
        nulos = df["ano_assinatura"].isna().sum()
        print(f"  [ANO] ano_assinatura: {df['ano_assinatura'].nunique()} anos únicos | "
              f"nulos: {nulos:,} | range: "
              f"{df['ano_assinatura'].min()} - {df['ano_assinatura'].max()}")

        antes = len(df)
        df_filtrado = df[df["ano_assinatura"].isin(FILTROS["anos_assinatura"])]
        if len(df_filtrado) == 0:
            print(f"  [AVISO] Filtro ano zerou convenios — mantendo todos ({antes:,})")
        else:
            df = df_filtrado
            print(f"  [FILTRO ANO ASSINATURA] {antes:,} → {len(df):,}")
    else:
        print("  [AVISO] Coluna 'dia_assin_conv' não encontrada — sem filtro de ano")

    # ── 1. Diagnóstico RAW ANTES de converter ────────────────────────────────────
    if "vl_saldo_conta" in df.columns:
        amostra = df["vl_saldo_conta"].dropna().astype(str).str.strip().head(5).tolist()
        print(f"  [SALDO RAW] Amostra bruta vl_saldo_conta: {amostra}")

    # ── 2. Converte valores ──────────────────────────────────────────────────────
    df = converter_valores(df)

    # ── 3. Alias vl_saldo_conta → valor_saldo_conta (APÓS converter) ────────────
    ALIASES_SALDO = [
        "vl_saldo_conta", "saldo_conta",
        "vl_saldo_ctabancaria", "valor_saldo_ctabancaria",
    ]
    for alias in ALIASES_SALDO:
        if alias in df.columns and "valor_saldo_conta" not in df.columns:
            df = df.rename(columns={alias: "valor_saldo_conta"})
            print(f"  [SALDO CONTA] '{alias}' → 'valor_saldo_conta'")
            break

    if "valor_saldo_conta" not in df.columns:
        df["valor_saldo_conta"] = 0.0
        print("  [SALDO CONTA] Coluna ausente — preenchida com 0.0")

    # ── 4. Clip negativos ────────────────────────────────────────────────────────
    df["valor_saldo_conta"] = pd.to_numeric(
        df["valor_saldo_conta"], errors="coerce"
    ).fillna(0.0).clip(lower=0)

    # ── 5. Validação cruzada saldo vs repasse (APÓS alias e converter) ───────────
    if "valor_saldo_conta" in df.columns and "vl_repasse_conv" in df.columns:
        repasse = pd.to_numeric(df["vl_repasse_conv"], errors="coerce").fillna(0.0)
        saldo   = df["valor_saldo_conta"]

        mask_invalido = saldo > (repasse * 1.1)
        qtd_invalidos = mask_invalido.sum()

        if qtd_invalidos > 0:
            print(f"  [VALIDAÇÃO] ⚠️ {qtd_invalidos} convênios com saldo > repasse")
            amostra_inv = df.loc[
                mask_invalido,
                ["nr_convenio", "vl_repasse_conv", "valor_saldo_conta"]
            ].head(5)
            print(f"  [VALIDAÇÃO] Amostra:\n{amostra_inv.to_string()}")
            df.loc[mask_invalido, "valor_saldo_conta"] = (
                df.loc[mask_invalido, "valor_saldo_conta"] / 100
            )
            print(f"  [VALIDAÇÃO] ✅ Corrigido — dividido por 100")
        else:
            print(f"  [VALIDAÇÃO] ✅ Todos os saldos dentro do esperado")

        saldo_pos = df["valor_saldo_conta"].clip(lower=0)
        print(f"  [SALDO CONTA] Após validação: R$ {saldo_pos.sum():,.2f}")

    # ── 6. Dedup por nr_convenio ─────────────────────────────────────────────────
    if "nr_convenio" in df.columns:
        antes_dedup = len(df)
        df = (
            df.sort_values("valor_saldo_conta", ascending=False, na_position="last")
              .drop_duplicates(subset=["nr_convenio"], keep="first")
              .reset_index(drop=True)
        )
        print(f"  [DEDUP nr_convenio] {antes_dedup:,} → {len(df):,}")
    else:
        print("  [AVISO] Coluna 'nr_convenio' não encontrada — dedup ignorado")

    saldo_total = df["valor_saldo_conta"].sum()
    print(f"  Convênios mantidos:  {len(df):,}")
    print(f"  Saldo conta total:   R$ {saldo_total:,.2f}  ← conferir vs Transferegov")

    return df

def processar_proposta(forcar: bool = False) -> pd.DataFrame | None:
    print("\n[PROPOSTA]")
    df = baixar_e_extrair("proposta", forcar)
    if df is None:
        return None

    df = normalizar_colunas(df)
    print(f"  Bruto: {len(df):,} linhas | Colunas: {list(df.columns[:10])}")

    # ← CORRIGIDO: usa detecção robusta com fallback heurístico
    df = filtrar_uf(df, "proposta", colunas_uf=["uf_proponente", "uf_propon"])

    if len(df) == 0:
        print("  [FALHA] DataFrame vazio após filtro UF.")
        # Diagnóstico extra: mostra colunas reais do arquivo
        df_raw = baixar_e_extrair("proposta", forcar=False)
        if df_raw is not None:
            df_raw = normalizar_colunas(df_raw)
            print(f"  [DEBUG] Todas as colunas: {list(df_raw.columns)}")
            for c in df_raw.columns:
                if "uf" in c.lower() or "estado" in c.lower():
                    print(f"  [DEBUG] '{c}' valores: "
                          f"{df_raw[c].dropna().unique()[:20]}")
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
            print(f"  [FILTRO ANO PROPOSTA] {antes:,} → {len(df):,}")

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

    if df_conv is None:
        print("\n[FALHA] siconv_convenio indisponivel.")
        return None

    if df_prop is None or len(df_prop) == 0:
        print("\n[FALHA] siconv_proposta indisponivel ou vazio após filtros.")
        return None

    df_base = df_prop.copy()
    print(f"\n[BASE] Propostas TO: {len(df_base):,}")

    # ── Join proposta → convenio ──────────────────────────────────────────
    CHAVES_POSSIVEIS = ["id_proposta", "nr_proposta"]

    col_prop_id = next((c for c in CHAVES_POSSIVEIS if c in df_base.columns), None)
    col_conv_id = next((c for c in CHAVES_POSSIVEIS if c in df_conv.columns), None)

    print(f"  [JOIN] Chaves disponíveis proposta: "
          f"{[c for c in CHAVES_POSSIVEIS if c in df_base.columns]}")
    print(f"  [JOIN] Chaves disponíveis convenio: "
          f"{[c for c in CHAVES_POSSIVEIS if c in df_conv.columns]}")
    print(f"  [JOIN] Usando → proposta='{col_prop_id}' | convenio='{col_conv_id}'")

    if col_prop_id and col_conv_id:
        antes_conv = len(df_conv)
        df_conv_join = df_conv.drop_duplicates(subset=[col_conv_id], keep="first")
        if antes_conv != len(df_conv_join):
            print(f"  [DEDUP JOIN] {antes_conv:,} → {len(df_conv_join):,} "
                f"(por '{col_conv_id}' para segurança do merge)")

        antes = len(df_base)
        df_base = pd.merge(
            df_base, df_conv_join,          
            left_on=col_prop_id, right_on=col_conv_id,
            how="left", suffixes=("", "_conv")
        )
        df_base = df_base.loc[:, ~df_base.columns.duplicated()]

        col_check = col_conv_id + "_conv" if col_conv_id + "_conv" \
                    in df_base.columns else col_conv_id
        match_rate = df_base[col_check].notna().sum()
        print(f"  [MERGE proposta↔convenio] {antes:,} → {len(df_base):,} | "
              f"com match: {match_rate:,}")
    else:
        print("  [AVISO] Nenhuma chave de join encontrada — sem merge com convenio")
        print(f"  Colunas proposta:  {list(df_prop.columns[:15])}")
        print(f"  Colunas convenio:  {list(df_conv.columns[:15])}")

    # ── Join base → emenda (com agregação por id_proposta) ───────────────
    if df_emenda is not None and len(df_emenda) > 0:
        col_base_em  = next((c for c in df_base.columns   if c == "id_proposta"), None)
        col_emend_id = next((c for c in df_emenda.columns if c == "id_proposta"), None)

        if col_base_em and col_emend_id:
            agg_dict = {}
            for campo, agg_fn in [
                ("nr_emenda",
                 lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("nome_parlamentar",
                 lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("tipo_parlamentar",
                 lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("ind_impositivo",
                 lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("beneficiario_emenda",
                 lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("cod_programa_emenda",
                 lambda x: " | ".join(x.dropna().astype(str).unique())),
                ("valor_repasse_proposta_emenda", "sum"),
                ("valor_repasse_emenda",          "sum"),
            ]:
                if campo in df_emenda.columns:
                    agg_dict[campo] = agg_fn

            df_emenda_agg = df_emenda.groupby(col_emend_id, as_index=False).agg(agg_dict)

            antes = len(df_base)
            df_base = pd.merge(
                df_base, df_emenda_agg,
                left_on=col_base_em, right_on=col_emend_id,
                how="left", suffixes=("", "_emenda")
            )
            df_base = df_base.loc[:, ~df_base.columns.duplicated()]

            com_emenda = df_base["nr_emenda"].notna().sum() \
                         if "nr_emenda" in df_base.columns else 0
            print(f"  [MERGE base↔emenda] {antes:,} → {len(df_base):,} | "
                  f"com emenda: {com_emenda:,}")
        else:
            print("  [AVISO] Chave 'id_proposta' não encontrada — pulando merge de emenda.")
    else:
        print("  [AVISO] df_emenda vazio ou None — pulando merge de emenda.")

    # ── Saldo em conta — alias defensivo ─────────────────────────────────
    ALIASES_SALDO = [
        "vl_saldo_conta", "saldo_conta",
        "vl_saldo_ctabancaria", "valor_saldo_ctabancaria",
    ]
    for alias in ALIASES_SALDO:
        if alias in df_base.columns and "valor_saldo_conta" not in df_base.columns:
            df_base = df_base.rename(columns={alias: "valor_saldo_conta"})
            print(f"  [SALDO CONTA] Alias '{alias}' → 'valor_saldo_conta'")
            break

    if "valor_saldo_conta" not in df_base.columns:
        df_base["valor_saldo_conta"] = 0.0
        print("  [SALDO CONTA] Coluna ausente — preenchida com 0.0")

    # ── Cruzamento com pagamento.csv ──────────────────────────────────────
    print("\n[PAGAMENTO] Iniciando cruzamento...")
    df_pag = baixar_pagamento(forcar=forcar)

    if df_pag is not None and not df_pag.empty:
        CHAVES_CONV = ["nr_convenio", "nr_conv"]
        col_base_nr = next((c for c in CHAVES_CONV if c in df_base.columns), None)

        if col_base_nr:
            df_base[col_base_nr] = df_base[col_base_nr].astype(str).str.strip()
            df_pag["nr_convenio"] = df_pag["nr_convenio"].astype(str).str.strip()

            antes = len(df_base)
            df_base = pd.merge(
                df_base, df_pag,
                left_on=col_base_nr, right_on="nr_convenio",
                how="left", suffixes=("", "_pag")
            )
            df_base = df_base.loc[:, ~df_base.columns.duplicated()]

            match_pag = df_base["valor_pago"].notna().sum() \
                        if "valor_pago" in df_base.columns else 0
            print(f"  [MERGE base↔pagamento] {antes:,} → {len(df_base):,} | "
                  f"com match: {match_pag:,}")
        else:
            print("  [PAGAMENTO] ⚠️ Chave 'nr_convenio' não encontrada em df_base.")
            df_base["valor_pago"] = 0.0
    else:
        print("  [PAGAMENTO] ⚠️ df_pag vazio — valor_pago zerado.")
        df_base["valor_pago"] = 0.0

    # ── Garante numérico em valor_pago e valor_saldo_conta ───────────────
    df_base["valor_pago"] = (
        pd.to_numeric(df_base["valor_pago"], errors="coerce").fillna(0.0)
    )
    df_base["valor_saldo_conta"] = (
        pd.to_numeric(df_base["valor_saldo_conta"], errors="coerce").fillna(0.0)
    )

    # ── Renomeia para nomes padronizados de saída ─────────────────────────
    df_base = renomear_colunas(df_base)

    # ── Garante tipos de ano corretos ─────────────────────────────────────
    for col in ["ano_proposta", "ano_assinatura"]:
        if col in df_base.columns:
            df_base[col] = pd.to_numeric(df_base[col], errors="coerce").astype("Int64")

    # ── Remove colunas duplicadas residuais ───────────────────────────────
    df_base = df_base.loc[:, ~df_base.columns.duplicated()]

    if len(df_base) == 0:
        print("\n[FALHA] DataFrame final vazio — CSV não será salvo.")
        return None

    # ── Salva CSV final ───────────────────────────────────────────────────
    saida = os.path.join(DATA_DIR, "discricionarias_to.csv")
    df_base.to_csv(saida, sep=";", index=False, encoding="utf-8-sig")

    # ── Relatório final ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  Arquivo:       {saida}")
    print(f"  Total linhas:  {len(df_base):,}")
    print(f"  Total colunas: {len(df_base.columns)}")
    print(f"  valor_pago:    R$ {df_base['valor_pago'].sum():,.2f}")
    print(f"  saldo_conta:   R$ {df_base['valor_saldo_conta'].sum():,.2f}")
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
