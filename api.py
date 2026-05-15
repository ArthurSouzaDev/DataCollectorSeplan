import requests
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import time
import json
from pathlib import Path

load_dotenv()

fundo_a_fundo   = os.getenv("URL_FUNDO_A_FUNDO")
transf_especial = os.getenv("URL_TRANSF_ESPECIAL")

CACHE_FILE = Path("cache_natureza.json")
OUTPUT_FUNDO = Path("fundo_a_fundo.csv")
OUTPUT_EMENDAS = Path("emendas_to.csv")
MAX_TENTATIVAS = 3
ESPERA_RETRY_SEGUNDOS = 5


def _carregar_cache() -> dict:
    if CACHE_FILE.exists():
        with CACHE_FILE.open(encoding="utf-8") as f:
            return json.load(f)
    return {}


def _salvar_cache() -> None:
    with CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(_cache_natureza, f, ensure_ascii=False)


_cache_natureza = _carregar_cache()


def validar_colunas(df: pd.DataFrame, obrigatorias: list[str], nome_dataset: str) -> None:
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        print(f"[{nome_dataset}] Dataset inválido. Colunas retornadas: {df.columns.tolist()}")
        raise Exception(f"[{nome_dataset}] Colunas obrigatórias ausentes: {faltantes}")


def validar_dataframe(df: pd.DataFrame, nome_dataset: str, colunas_obrigatorias: list[str]) -> None:
    print(f"[{nome_dataset}] Registros após tratamento: {len(df):,}")
    print(f"[{nome_dataset}] Colunas retornadas: {df.columns.tolist()}")
    if df.empty:
        print(f"[{nome_dataset}] Dataset inválido: DataFrame vazio.")
        raise Exception(f"[{nome_dataset}] DataFrame vazio após extração da API.")
    validar_colunas(df, colunas_obrigatorias, nome_dataset)

def limpar_cnpj(cnpj: str) -> str:
    if not cnpj:
        return ""
    return cnpj.replace(".", "").replace("/", "").replace("-", "").strip()

def get_natureza_juridica(cnpj: str) -> str:
    cnpj_limpo = limpar_cnpj(cnpj)

    if not cnpj_limpo or len(cnpj_limpo) != 14:
        return "Não informado"

    # Retorna do cache se já consultado
    if cnpj_limpo in _cache_natureza:
        return _cache_natureza[cnpj_limpo]

    try:
        r = requests.get(
            f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}",
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            natureza = data.get("natureza_juridica", "Não encontrado")
        else:
            natureza = "Não encontrado"
    except Exception as e:
        print(f"  ⚠️ Erro CNPJ {cnpj_limpo}: {e}")
        natureza = "Erro na consulta"

    _cache_natureza[cnpj_limpo] = natureza
    time.sleep(0.3)  # respeita rate limit da API
    return natureza

def enriquecer_natureza(df: pd.DataFrame, coluna_cnpj: str) -> pd.DataFrame:
    """Consulta a BrasilAPI para cada CNPJ único e adiciona coluna natureza_juridica"""
    validar_colunas(df, [coluna_cnpj], "ENRIQUECIMENTO")
    cnpjs_unicos = df[coluna_cnpj].dropna().unique()
    total = len(cnpjs_unicos)

    print(f"\n🔍 Consultando natureza jurídica para {total} CNPJs únicos...")

    for i, cnpj in enumerate(cnpjs_unicos, 1):
        natureza = get_natureza_juridica(cnpj)
        print(f"  [{i}/{total}] {cnpj} → {natureza}")

    df["natureza_juridica"] = df[coluna_cnpj].apply(get_natureza_juridica)
    return df



# ─────────────────────────────────────────────
def _request_com_retry(endpoint: str, params: dict, nome_dataset: str) -> requests.Response:
    if not endpoint:
        raise ValueError(f"[{nome_dataset}] Endpoint não configurado.")

    ultimo_erro = None
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            print(f"[{nome_dataset}] Requisição tentativa {tentativa}/{MAX_TENTATIVAS} | params={params}")
            response = requests.get(endpoint, params=params, timeout=60)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            ultimo_erro = e
            status = getattr(getattr(e, "response", None), "status_code", "sem status")
            print(f"[{nome_dataset}] Tentativa {tentativa} falhou | HTTP={status} | erro={e}")
            if tentativa < MAX_TENTATIVAS:
                time.sleep(ESPERA_RETRY_SEGUNDOS)

    raise RuntimeError(
        f"[{nome_dataset}] Falha após {MAX_TENTATIVAS} tentativas. Último erro: {ultimo_erro}"
    )


def extrair_dados(endpoint, params, nome_dataset):
    todos_os_dados = []
    limit = 1000
    offset = 0

    while True:
        params_local = params.copy()
        params_local['limit'] = limit
        params_local['offset'] = offset

        response = _request_com_retry(endpoint, params_local, nome_dataset)

        dados = response.json()

        if not dados:
            print(f"[{nome_dataset}] Página offset {offset} sem registros.")
            break

        todos_os_dados.extend(dados)
        print(f"[{nome_dataset}] Página offset {offset} — {len(dados)} registros recebidos")

        if len(dados) < limit:
            break

        offset += limit

    print(f"[{nome_dataset}] Total extraído: {len(todos_os_dados)} registros")
    return todos_os_dados


# ─────────────────────────────────────────────
def tratar_float(valor):
    if valor is None:
        return 0.0
    try:
        return float(valor)
    except:
        return 0.0

def tratar_data(data_str):
    if not data_str:
        return ""
    try:
        return datetime.strptime(data_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        return ""


# ─────────────────────────────────────────────
# 🔷 TRATAMENTO — FUNDO A FUNDO
# ─────────────────────────────────────────────
def tratar_dados(dados):
    dados_tratados = []

    for item in dados:
        uf = (item.get("uf_ente_recebedor_plano_acao") or "").strip().upper()

        if uf != "TO":
            continue

        registro = {
            "codigo_plano":       item.get("codigo_plano_acao"),
            "situacao":           item.get("situacao_plano_acao") or "Não informado",
            "data_inicio":        tratar_data(item.get("data_inicio_vigencia_plano_acao")),
            "data_fim":           tratar_data(item.get("data_fim_vigencia_plano_acao")),
            "ente_recebedor":     item.get("nome_ente_recebedor_plano_acao") or "Não informado",
            "municipio":          item.get("nome_municipio_ente_recebedor_plano_acao") or "Não informado",
            "uf":                 uf,
            "cnpj_recebedor":     item.get("cnpj_ente_recebedor_plano_acao") or "Não informado",
            "fundo_repassador":   item.get("nome_fundo_repassador_plano_acao") or "Não informado",
            "orgao_repassador":   item.get("nome_orgao_repassador_plano_acao") or "Não informado",
            "sigla_orgao":        item.get("sigla_orgao_repassador_plano_acao") or "Não informado",
            "fundo_recebedor":    item.get("nome_fundo_recebedor_plano_acao") or "Não informado",
            "valor_emenda":       tratar_float(item.get("valor_repasse_emenda_plano_acao")),
            "valor_especifico":   tratar_float(item.get("valor_repasse_especifico_plano_acao")),
            "valor_voluntario":   tratar_float(item.get("valor_repasse_voluntario_plano_acao")),
            "valor_total_repasse":tratar_float(item.get("valor_total_repasse_plano_acao")),
            "valor_total_plano":  tratar_float(item.get("valor_total_plano_acao")),
            "saldo_disponivel":   tratar_float(item.get("valor_saldo_disponivel_plano_acao")),
        }

        dados_tratados.append(registro)

    return dados_tratados


#  TRATAMENTO — EMENDAS PARLAMENTARES
# ─────────────────────────────────────────────
def tratar_dados_emenda(dados):
    dados_tratados = []

    for item in dados:
        uf = (item.get("uf_beneficiario_plano_acao") or "").strip().upper()

        if uf != "TO":
            continue

        registro = {
            "codigo_plano":           item.get("codigo_plano_acao"),
            "ano_plano":              item.get("ano_plano_acao") or 0,
            "modalidade":             item.get("modalidade_plano_acao") or "Não informado",
            "situacao":               item.get("situacao_plano_acao") or "Não informado",
            "beneficiario":           item.get("nome_beneficiario_plano_acao") or "Não informado",
            "cnpj_beneficiario":      item.get("cnpj_beneficiario_plano_acao") or "Não informado",
            "uf":                     uf,
            "banco":                  item.get("nome_banco_plano_acao") or "Não informado",
            "agencia":                str(item.get("numero_agencia_plano_acao") or ""),
            "conta":                  str(item.get("numero_conta_plano_acao") or ""),
            "dv_agencia":             item.get("dv_agencia_plano_acao") or "",
            "dv_conta":               item.get("dv_conta_plano_acao") or "",
            "parlamentar":            item.get("nome_parlamentar_emenda_plano_acao") or "Não informado",
            "ano_emenda":             item.get("ano_emenda_parlamentar_plano_acao") or "",
            "numero_emenda":          item.get("numero_emenda_parlamentar_plano_acao") or "",
            "codigo_emenda":          item.get("codigo_emenda_parlamentar_formatado_plano_acao") or "",
            "area_politica":          item.get("codigo_descricao_areas_politicas_publicas_plano_acao") or "Não informado",
            "programa_orcamentario":  item.get("descricao_programacao_orcamentaria_plano_acao") or "Não informado",
            "valor_custeio":          tratar_float(item.get("valor_custeio_plano_acao")),
            "valor_investimento":     tratar_float(item.get("valor_investimento_plano_acao")),
        }

        dados_tratados.append(registro)

    return dados_tratados

# ─────────────────────────────────────────────
#  EXECUÇÃO — 
# ─────────────────────────────────────────────
if __name__ == "__main__":
    params_fundo    = {'uf_ente_recebedor_plano_acao': 'eq.TO'}
    params_especial = {'uf_beneficiario_plano_acao':   'eq.TO'}

    # Extração
    dados          = extrair_dados(fundo_a_fundo, params_fundo, "FUNDO_A_FUNDO")
    dados_tratados = tratar_dados(dados)

    dados_emenda           = extrair_dados(transf_especial, params_especial, "EMENDAS")
    dados_tratados_emendas = tratar_dados_emenda(dados_emenda)

    # DataFrames
    df_fundo  = pd.DataFrame(dados_tratados)
    df_emenda = pd.DataFrame(dados_tratados_emendas)

    validar_dataframe(df_fundo, "FUNDO_A_FUNDO", ["cnpj_recebedor"])
    validar_dataframe(df_emenda, "EMENDAS", ["cnpj_beneficiario"])

    df_fundo["data_inicio"] = pd.to_datetime(df_fundo["data_inicio"], errors="coerce")
    df_fundo["data_fim"]    = pd.to_datetime(df_fundo["data_fim"],    errors="coerce")

    # Enriquecimento
    print("\n━━━ Enriquecendo FUNDO A FUNDO ━━━")
    df_fundo  = enriquecer_natureza(df_fundo,  "cnpj_recebedor")

    print("\n━━━ Enriquecendo EMENDAS ━━━")
    df_emenda = enriquecer_natureza(df_emenda, "cnpj_beneficiario")

    # Exportação segura: só substitui arquivos válidos ao final de uma execução consistente.
    tmp_fundo = OUTPUT_FUNDO.with_suffix(".csv.tmp")
    tmp_emendas = OUTPUT_EMENDAS.with_suffix(".csv.tmp")
    df_fundo.to_csv(tmp_fundo, index=False, sep=";", date_format="%Y-%m-%d")
    df_emenda.to_csv(tmp_emendas, index=False, sep=";")
    tmp_fundo.replace(OUTPUT_FUNDO)
    tmp_emendas.replace(OUTPUT_EMENDAS)

    print("\n✅ CSVs exportados com sucesso!")
    _salvar_cache()
