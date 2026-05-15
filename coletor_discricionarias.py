from __future__ import annotations

import gc
import io
import logging
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_discricionarias"
RAW_DIR = DATA_DIR / "cache_bruto"
EXTRACTED_DIR = DATA_DIR / "extraidos"
PROCESSED_DIR = DATA_DIR / "processados"
OUTPUT_PATH = PROCESSED_DIR / "discricionarias_to.parquet"
CHUNK_SIZE = 50_000

URLS = {
    "convenio": "https://repositorio.dados.gov.br/seges/detru/siconv_convenio.csv.zip",
    "proposta": "https://repositorio.dados.gov.br/seges/detru/siconv_proposta.csv.zip",
    "emenda": "https://repositorio.dados.gov.br/seges/detru/siconv_emenda.csv.zip",
    "pagamento": "https://repositorio.dados.gov.br/seges/detru/siconv_pagamento.csv.zip",
    "programa": "https://repositorio.dados.gov.br/seges/detru/siconv_programa.csv.zip",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )
}

USECOLS = {
    "convenio": {
        "NR_CONVENIO", "ID_PROPOSTA", "DIA_ASSIN_CONV", "SIT_CONVENIO",
        "SUBSITUACAO_CONV", "DIA_INIC_VIGENC_CONV", "DIA_FIM_VIGENC_CONV",
        "VL_GLOBAL_CONV", "VL_REPASSE_CONV", "VL_CONTRAPARTIDA_CONV",
        "VL_EMPENHADO_CONV", "VL_DESEMBOLSADO_CONV", "VL_SALDO_REMAN_TESOURO",
        "VL_INGRESSO_CONTRAPARTIDA", "VL_RENDIMENTO_APLICACAO",
        "VL_SALDO_REMAN_CONVENENTE", "VL_SALDO_CONTA",
    },
    "proposta": {
        "ID_PROPOSTA", "UF_PROPONENTE", "MUNIC_PROPONENTE", "NR_PROPOSTA",
        "ANO_PROP", "MODALIDADE", "NM_PROPONENTE", "NATUREZA_JURIDICA",
        "DESC_ORGAO_SUP", "DESC_ORGAO",
    },
    "emenda": {
        "ID_PROPOSTA", "NR_EMENDA", "NOME_PARLAMENTAR", "TIPO_PARLAMENTAR",
        "IND_IMPOSITIVO", "BENEFICIARIO_EMENDA", "COD_PROGRAMA_EMENDA",
        "VALOR_REPASSE_PROPOSTA_EMENDA", "VALOR_REPASSE_EMENDA",
    },
    "pagamento": {"NR_CONVENIO", "VL_PAGO"},
}

REQUIRED_COLUMNS = {
    "convenio": {"nr_convenio", "id_proposta"},
    "proposta": {"id_proposta", "uf_proponente"},
    "emenda": {"id_proposta"},
    "pagamento": {"nr_convenio"},
}

MONETARY_COLUMNS = {
    "vl_global_conv", "vl_repasse_conv", "vl_contrapartida_conv", "vl_empenhado_conv",
    "vl_desembolsado_conv", "vl_saldo_reman_tesouro", "vl_ingresso_contrapartida",
    "vl_rendimento_aplicacao", "vl_saldo_reman_convenente", "vl_saldo_conta",
    "valor_repasse_proposta_emenda", "valor_repasse_emenda", "vl_pago",
}

OUTPUT_RENAME = {
    "sit_convenio": "situacao",
    "subsituacao_conv": "subsituacao",
    "dia_assin_conv": "dt_assinatura",
    "dia_inic_vigenc_conv": "dt_inicio_vigencia",
    "dia_fim_vigenc_conv": "dt_fim_vigencia",
    "vl_global_conv": "valor_global",
    "vl_repasse_conv": "valor_repasse",
    "vl_contrapartida_conv": "valor_contrapartida",
    "vl_empenhado_conv": "valor_empenhado",
    "vl_desembolsado_conv": "valor_desembolsado",
    "vl_saldo_reman_tesouro": "valor_saldo_tesouro",
    "vl_saldo_conta": "valor_saldo_conta",
    "municip_proponente": "municipio_beneficiario",
    "munic_proponente": "municipio_beneficiario",
    "nm_proponente": "proponente",
    "desc_orgao_sup": "orgao_superior",
    "desc_orgao": "orgao_concedente",
    "ano_prop": "ano_proposta",
    "nome_parlamentar": "parlamentar",
    "valor_repasse_emenda": "valor_emenda",
    "vl_pago": "valor_pago",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_discricionarias")


def _ensure_dirs() -> None:
    for directory in (RAW_DIR, EXTRACTED_DIR, PROCESSED_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False).str.replace("-", "_", regex=False)
    )
    return df


def _estimate_memory_mb(df: pd.DataFrame) -> float:
    return df.memory_usage(index=True, deep=True).sum() / (1024 * 1024)


def _detect_sep(path: Path) -> str:
    with path.open("r", encoding="utf-8-sig", errors="ignore") as handle:
        first_line = handle.readline()
    return ";" if ";" in first_line else ("\t" if "\t" in first_line else ",")


def _read_chunks(dataset: str) -> Iterable[pd.DataFrame]:
    csv_path = EXTRACTED_DIR / f"siconv_{dataset}.csv"
    sep = _detect_sep(csv_path)
    usecols = USECOLS.get(dataset)
    return pd.read_csv(
        csv_path,
        sep=sep,
        encoding="utf-8-sig",
        usecols=lambda col: col.strip().upper() in usecols if usecols else True,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        on_bad_lines="skip",
    )


def _convert_money(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    sample = text.dropna().head(100)
    has_comma = sample.str.contains(",", regex=False).any()
    has_dot = sample.str.contains(".", regex=False).any()
    multiple_dots = sample.str.count(r"\.").max() > 1 if not sample.empty else False
    if has_comma and has_dot:
        text = text.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    elif has_comma:
        text = text.str.replace(",", ".", regex=False)
    elif multiple_dots:
        text = text.str.replace(".", "", regex=False)
    return pd.to_numeric(text, errors="coerce")


def _validate_chunk(dataset: str, df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS.get(dataset, set()) - set(df.columns)
    if missing:
        raise ValueError(f"{dataset}: colunas obrigatórias ausentes: {sorted(missing)}")
    invalid_money = 0
    for col in MONETARY_COLUMNS & set(df.columns):
        original = df[col].astype(str).str.strip()
        converted = _convert_money(df[col])
        invalid_money += int((converted.isna() & original.ne("") & original.ne("nan")).sum())
    logger.info("[%s] chunk validado | linhas=%s | valores monetários inválidos=%s | memória=%.2f MB",
                dataset, f"{len(df):,}", f"{invalid_money:,}", _estimate_memory_mb(df))


def download_raw_data() -> dict[str, Path]:
    """Baixa novamente todos os ZIPs do GOV e sempre sobrescreve arquivos antigos."""
    _ensure_dirs()
    downloaded: dict[str, Path] = {}
    for dataset, url in URLS.items():
        started = time.perf_counter()
        zip_path = RAW_DIR / f"siconv_{dataset}.csv.zip"
        tmp_path = zip_path.with_suffix(zip_path.suffix + ".tmp")
        zip_path.unlink(missing_ok=True)
        logger.info("[%s] download iniciado", dataset)
        try:
            with requests.get(url, headers=HEADERS, timeout=600, stream=True) as response:
                response.raise_for_status()
                with tmp_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            tmp_path.replace(zip_path)
            size_mb = zip_path.stat().st_size / (1024 * 1024)
            logger.info("[%s] download finalizado | tamanho=%.2f MB | tempo=%.2fs",
                        dataset, size_mb, time.perf_counter() - started)
            downloaded[dataset] = zip_path
        except Exception:
            tmp_path.unlink(missing_ok=True)
            logger.exception("[%s] falha no download; dataset será ignorado nesta execução", dataset)
    return downloaded


def extract_raw_data(zip_files: dict[str, Path] | None = None) -> dict[str, Path]:
    """Extrai os CSVs baixados, validando ZIPs corrompidos antes da leitura."""
    _ensure_dirs()
    extracted: dict[str, Path] = {}
    zip_files = zip_files or {k: RAW_DIR / f"siconv_{k}.csv.zip" for k in URLS}
    for dataset, zip_path in zip_files.items():
        started = time.perf_counter()
        out_path = EXTRACTED_DIR / f"siconv_{dataset}.csv"
        out_path.unlink(missing_ok=True)
        try:
            with zipfile.ZipFile(zip_path) as archive:
                if archive.testzip() is not None:
                    raise zipfile.BadZipFile("arquivo ZIP corrompido")
                members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if not members:
                    raise ValueError("ZIP sem CSV")
                with archive.open(members[0]) as source, out_path.open("wb") as target:
                    shutil.copyfileobj(source, target)
            logger.info("[%s] extração finalizada | tamanho=%.2f MB | tempo=%.2fs",
                        dataset, out_path.stat().st_size / (1024 * 1024), time.perf_counter() - started)
            extracted[dataset] = out_path
        except Exception:
            logger.exception("[%s] falha na extração; dataset será ignorado nesta execução", dataset)
    return extracted


def process_convenio() -> pd.DataFrame:
    rows = 0
    best_rows: dict[str, dict] = {}
    for chunk_no, chunk in enumerate(_read_chunks("convenio"), start=1):
        chunk = _normalize_columns(chunk)
        _validate_chunk("convenio", chunk)
        chunk["ano_assinatura"] = pd.to_datetime(chunk.get("dia_assin_conv"), dayfirst=True, errors="coerce").dt.year
        for col in MONETARY_COLUMNS & set(chunk.columns):
            chunk[col] = _convert_money(chunk[col]).fillna(0.0)
        chunk["nr_convenio"] = chunk["nr_convenio"].astype(str).str.replace(r"\.0$", "", regex=True)
        for record in chunk.to_dict("records"):
            key = record["nr_convenio"]
            current = best_rows.get(key)
            if current is None or record.get("vl_saldo_conta", 0.0) > current.get("vl_saldo_conta", 0.0):
                best_rows[key] = record
        rows += len(chunk)
        logger.info("[convenio] chunk=%s processado | acumulado=%s", chunk_no, f"{rows:,}")
    df = pd.DataFrame(best_rows.values()) if best_rows else pd.DataFrame()
    logger.info("[convenio] linhas processadas=%s | anos disponíveis=%s", f"{rows:,}",
                sorted(df["ano_assinatura"].dropna().astype(int).unique().tolist()) if "ano_assinatura" in df else [])
    return df


def process_proposta() -> pd.DataFrame:
    rows = 0
    parts: list[pd.DataFrame] = []
    for chunk_no, chunk in enumerate(_read_chunks("proposta"), start=1):
        chunk = _normalize_columns(chunk)
        _validate_chunk("proposta", chunk)
        chunk = chunk[chunk["uf_proponente"].astype(str).str.upper().eq("TO")].copy()
        rows += len(chunk)
        parts.append(chunk)
        logger.info("[proposta] chunk=%s processado | linhas TO acumuladas=%s", chunk_no, f"{rows:,}")
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    logger.info("[proposta] linhas processadas=%s | anos disponíveis=%s", f"{rows:,}",
                sorted(pd.to_numeric(df.get("ano_prop"), errors="coerce").dropna().astype(int).unique().tolist()) if not df.empty else [])
    return df


def process_emenda() -> pd.DataFrame:
    rows = 0
    aggregates: dict[str, dict] = {}
    for chunk_no, chunk in enumerate(_read_chunks("emenda"), start=1):
        chunk = _normalize_columns(chunk)
        _validate_chunk("emenda", chunk)
        for col in MONETARY_COLUMNS & set(chunk.columns):
            chunk[col] = _convert_money(chunk[col]).fillna(0.0)
        for record in chunk.to_dict("records"):
            key = record["id_proposta"]
            agg = aggregates.setdefault(
                key,
                {
                    "id_proposta": key,
                    "nr_emenda": set(),
                    "nome_parlamentar": set(),
                    "tipo_parlamentar": set(),
                    "valor_repasse_emenda": 0.0,
                },
            )
            for field in ("nr_emenda", "nome_parlamentar", "tipo_parlamentar"):
                value = record.get(field)
                if pd.notna(value):
                    agg[field].add(str(value))
            agg["valor_repasse_emenda"] += float(record.get("valor_repasse_emenda", 0.0) or 0.0)
        rows += len(chunk)
        logger.info("[emenda] chunk=%s processado | acumulado=%s", chunk_no, f"{rows:,}")
    result = pd.DataFrame(
        {
            **agg,
            "nr_emenda": " | ".join(sorted(agg["nr_emenda"])),
            "nome_parlamentar": " | ".join(sorted(agg["nome_parlamentar"])),
            "tipo_parlamentar": " | ".join(sorted(agg["tipo_parlamentar"])),
        }
        for agg in aggregates.values()
    )
    logger.info("[emenda] linhas processadas=%s | propostas agregadas=%s", f"{rows:,}", f"{len(result):,}")
    return result


def process_pagamento() -> pd.DataFrame:
    rows = 0
    totals: dict[str, float] = {}
    for chunk_no, chunk in enumerate(_read_chunks("pagamento"), start=1):
        chunk = _normalize_columns(chunk)
        _validate_chunk("pagamento", chunk)
        chunk["nr_convenio"] = chunk["nr_convenio"].astype(str).str.replace(r"\.0$", "", regex=True)
        chunk["vl_pago"] = _convert_money(chunk["vl_pago"]).fillna(0.0)
        grouped = chunk.groupby("nr_convenio")["vl_pago"].sum()
        for key, value in grouped.items():
            totals[key] = totals.get(key, 0.0) + float(value)
        rows += len(chunk)
        logger.info("[pagamento] chunk=%s processado | acumulado=%s", chunk_no, f"{rows:,}")
    if not totals:
        return pd.DataFrame(columns=["nr_convenio", "vl_pago"])
    df = pd.DataFrame({"nr_convenio": list(totals), "vl_pago": list(totals.values())})
    logger.info("[pagamento] linhas processadas=%s | convênios agregados=%s", f"{rows:,}", f"{len(df):,}")
    return df


def generate_outputs() -> pd.DataFrame:
    """Gera o parquet final; datasets auxiliares podem falhar sem derrubar a execução inteira."""
    started = time.perf_counter()
    results: dict[str, pd.DataFrame] = {}
    for name, processor in {
        "proposta": process_proposta,
        "convenio": process_convenio,
        "emenda": process_emenda,
        "pagamento": process_pagamento,
    }.items():
        try:
            results[name] = processor()
        except Exception:
            logger.exception("[%s] falhou no processamento; seguindo com os demais datasets", name)
            results[name] = pd.DataFrame()

    proposta = results["proposta"]
    if proposta.empty:
        raise RuntimeError("Não foi possível gerar a base final sem propostas válidas.")

    final_df = proposta.copy()
    convenio = results["convenio"]
    if not convenio.empty:
        final_df = final_df.merge(convenio, on="id_proposta", how="left", suffixes=("", "_conv"))
    else:
        logger.warning("[convenio] ausente; saída será gerada com colunas financeiras padrão")

    emenda = results["emenda"]
    if not emenda.empty:
        final_df = final_df.merge(emenda, on="id_proposta", how="left")

    pagamento = results["pagamento"]
    if not pagamento.empty and "nr_convenio" in final_df.columns:
        final_df = final_df.merge(pagamento, on="nr_convenio", how="left")

    final_df = final_df.rename(columns=OUTPUT_RENAME)
    for required, default in {
        "valor_pago": 0.0,
        "valor_saldo_conta": 0.0,
        "situacao": "Não informado",
        "municipio_beneficiario": "Não informado",
        "orgao_concedente": "Não informado",
        "valor_global": 0.0,
        "valor_repasse": 0.0,
        "natureza_juridica": "Não informado",
    }.items():
        if required not in final_df.columns:
            final_df[required] = default

    money_cols = [col for col in final_df.columns if col.startswith("valor_")]
    for col in money_cols:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce").fillna(0.0)
    if "ano_proposta" in final_df.columns:
        final_df["ano_proposta"] = pd.to_numeric(final_df["ano_proposta"], errors="coerce").astype("Int64")
    if "ano_assinatura" in final_df.columns:
        final_df["ano_assinatura"] = pd.to_numeric(final_df["ano_assinatura"], errors="coerce").astype("Int64")

    final_df.to_parquet(OUTPUT_PATH, index=False, compression="snappy")
    logger.info("[output] parquet salvo | caminho=%s | linhas=%s | colunas=%s | memória estimada=%.2f MB | tempo=%.2fs",
                OUTPUT_PATH, f"{len(final_df):,}", len(final_df.columns), _estimate_memory_mb(final_df),
                time.perf_counter() - started)
    return final_df


def run_etl() -> pd.DataFrame:
    _ensure_dirs()
    for dataset in URLS:
        (RAW_DIR / f"siconv_{dataset}.csv.zip").unlink(missing_ok=True)
        (EXTRACTED_DIR / f"siconv_{dataset}.csv").unlink(missing_ok=True)
    downloaded = download_raw_data()
    extract_raw_data(downloaded)
    result = generate_outputs()
    gc.collect()
    return result


if __name__ == "__main__":
    run_etl()
