"""ETL das transferências discricionárias e legais (Tocantins) via API pública do Transferegov.

Fonte: https://api-publica.transferegov.gestao.gov.br/discricionarias
Substitui os ZIPs do SICONV, removidos da fonte original.

Limitações da API (documentadas no openapi.json):
- Não há situação de convênio (SIT_CONVENIO), valor desembolsado, valor pago,
  saldo em conta nem vínculo proposta→emenda. Essas colunas saem zeradas e a
  base é marcada no log.
- O número do convênio vem dos planos de trabalho/metas (`meta-crono-fisico`),
  que só preenchem `nr_convenio` em parte dos registros. A coluna `fase` usa esse
  campo: "Convênio" quando existe número de convênio, "Proposta" caso contrário.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data_discricionarias" / "processados"
OUTPUT_PATH = PROCESSED_DIR / "discricionarias_to.parquet"

BASE_URL = "https://api-publica.transferegov.gestao.gov.br/discricionarias"
PAGE_SIZE = 200  # limite da API: 300 ou mais retorna 422
MAX_TENTATIVAS = 3
ESPERA_RETRY_SEGUNDOS = 5
TIMEOUT_SEGUNDOS = 90
HEADERS = {
    "User-Agent": "DataCollectorSeplan/1.0 (coleta automatizada)",
    "Accept": "application/json",
}

# Campos monetários que chegam como texto ("132897.0") na API.
CAMPOS_MONETARIOS_PROPOSTA = {
    "vl_global_prop": "valor_global",
    "vl_repasse_prop": "valor_repasse",
    "vl_contrapartida_prop": "valor_contrapartida",
}

# Colunas que o painel usa, mas que a API não fornece. Ficam zeradas (ou vazias).
COLUNAS_SEM_FONTE_NA_API = {
    "valor_desembolsado": 0.0,
    "valor_pago": 0.0,
    "valor_saldo_conta": 0.0,
    "valor_saldo_tesouro": 0.0,
    "vl_saldo_reman_convenente": 0.0,
    "vl_rendimento_aplicacao": 0.0,
    "vl_ingresso_contrapartida": 0.0,
    "valor_emenda": 0.0,
    "parlamentar": "Não informado",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_discricionarias")


def _get_com_retry(endpoint: str, params: dict) -> dict:
    ultimo_erro: Exception | None = None
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            response = requests.get(
                f"{BASE_URL}/{endpoint}", params=params, headers=HEADERS, timeout=TIMEOUT_SEGUNDOS
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as erro:
            ultimo_erro = erro
            logger.warning("[%s] tentativa %s/%s falhou: %s", endpoint, tentativa, MAX_TENTATIVAS, erro)
            if tentativa < MAX_TENTATIVAS:
                time.sleep(ESPERA_RETRY_SEGUNDOS)
    raise RuntimeError(f"[{endpoint}] falha após {MAX_TENTATIVAS} tentativas: {ultimo_erro}")


def fetch_all(endpoint: str, params: dict) -> list[dict]:
    """Percorre todas as páginas de um endpoint paginado da API."""
    registros: list[dict] = []
    pagina = 1
    while True:
        corpo = _get_com_retry(endpoint, {**params, "pagina": pagina, "tamanho_da_pagina": PAGE_SIZE})
        dados = corpo.get("data") or []
        registros.extend(dados)
        total_paginas = corpo.get("total_pages") or 0
        if pagina % 20 == 0 or pagina >= total_paginas:
            logger.info("[%s] página %s/%s | acumulado=%s", endpoint, pagina, total_paginas, f"{len(registros):,}")
        if not dados or pagina >= total_paginas:
            break
        pagina += 1
    return registros


def _to_float(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(serie.astype("string").str.strip(), errors="coerce")


def _propostas_to() -> pd.DataFrame:
    dados = fetch_all("propostas", {"uf_proponente": "TO"})
    df = pd.DataFrame(dados)
    if df.empty:
        raise RuntimeError("API não retornou propostas de Tocantins.")
    df = df.drop_duplicates(subset="id_proposta", keep="last")
    logger.info("[propostas] TO únicas=%s", f"{len(df):,}")
    return df


def _metas_com_convenio() -> pd.DataFrame:
    """Uma linha por proposta, com o número e a data de assinatura do convênio (quando houver)."""
    dados = fetch_all("meta-crono-fisico", {"uf_meta": "TO"})
    metas = pd.DataFrame(dados, columns=["id_proposta", "nr_convenio", "data_assin_conv"])
    if metas.empty:
        logger.warning("[metas] API não retornou metas de Tocantins; nenhuma proposta terá convênio.")
        return pd.DataFrame(columns=["id_proposta", "nr_convenio", "data_assin_conv"])
    metas["nr_convenio"] = metas["nr_convenio"].astype("string").str.strip().replace({"": pd.NA})
    metas = metas[metas["nr_convenio"].notna()]
    # `first` ignora nulos: fica com o primeiro número de convênio não vazio de cada proposta.
    agregado = metas.groupby("id_proposta", as_index=False).agg(
        nr_convenio=("nr_convenio", "first"),
        data_assin_conv=("data_assin_conv", "min"),
    )
    logger.info("[metas] propostas com número de convênio=%s", f"{len(agregado):,}")
    return agregado


def build_output() -> pd.DataFrame:
    started = time.perf_counter()
    propostas = _propostas_to()
    convenios = _metas_com_convenio()

    df = propostas.merge(convenios, on="id_proposta", how="left")

    df["fase"] = "Proposta"
    df.loc[df["nr_convenio"].notna(), "fase"] = "Convênio"

    # O painel oficial (dd-publico.serpro.gov.br) agrupa a situação pelo grupo da proposta
    # (Aprovadas / Rejeitadas / Cadastradas / Em Análise / Em Complementação), não pelo detalhe.
    situacao = df["sit_proposta_grupo"].astype("string").str.strip()
    df["situacao"] = situacao.replace({"": pd.NA}).fillna("Não informado")

    df["dt_assinatura"] = pd.to_datetime(df["data_assin_conv"], errors="coerce")
    df["ano_assinatura"] = df["dt_assinatura"].dt.year.astype("Int64")
    df["ano_proposta"] = pd.to_numeric(df["ano_prop"], errors="coerce").astype("Int64")

    df = df.rename(
        columns={
            "munic_proponente": "municipio_beneficiario",
            "nm_proponente": "proponente",
            "desc_orgao_sup": "orgao_superior",
            "desc_orgao": "orgao_concedente",
            **CAMPOS_MONETARIOS_PROPOSTA,
        }
    )
    for coluna in CAMPOS_MONETARIOS_PROPOSTA.values():
        df[coluna] = _to_float(df[coluna]).fillna(0.0)

    for coluna, padrao in COLUNAS_SEM_FONTE_NA_API.items():
        df[coluna] = padrao

    for coluna in ("municipio_beneficiario", "proponente", "orgao_superior", "orgao_concedente",
                   "natureza_juridica", "modalidade"):
        df[coluna] = df[coluna].astype("string").fillna("Não informado")

    df = df.drop(columns=[c for c in ("sit_proposta", "ano_prop", "data_assin_conv") if c in df])
    df["sit_proposta_grupo"] = df["sit_proposta_grupo"].astype("string").fillna("Não informado")

    convenios_n = int(df["fase"].eq("Convênio").sum())
    logger.info(
        "[output] registros=%s | convênios=%s | propostas=%s | colunas sem fonte na API=%s",
        f"{len(df):,}", f"{convenios_n:,}", f"{len(df) - convenios_n:,}", len(COLUNAS_SEM_FONTE_NA_API),
    )

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False, compression="snappy")
    logger.info("[output] parquet salvo | caminho=%s | tempo=%.2fs", OUTPUT_PATH, time.perf_counter() - started)
    return df


def run_etl() -> pd.DataFrame:
    return build_output()


if __name__ == "__main__":
    run_etl()
