"""Coleta de convênios de Tocantins pela API do Portal da Transparência (CGU).

Motivo: a API do Transferegov não expõe situação de convênio nem valor liberado.
Esses campos estão em `/api-de-dados/convenios`, que exige chave (header
`chave-api-dados`), lida da variável de ambiente PORTAL_TRANSPARENCIA_KEY.

Regras da API (confirmadas com a chave em 03/11/2026):
- Filtro só por UF (uf=TO) é aceito, sem data. Pagina com 15 itens por página.
- Filtro por período aceita até 1 mês (dataInicial/dataFinal pela dataReferencia do convênio).
- Tocantins tinha 11.519 convênios (768 páginas) nessa data.

Modos:
- backfill: lista todos os convênios de TO páginas 1..N. É resumível: o progresso
  (última página) fica salvo em dataset/portal_convenios_to.json.
- incremental (padrão): reconsulta os últimos DIAS_INCREMENTAIS dias pelo período
  e atualiza os registros pelo id. Exige backfill concluído.

Sem a chave, o módulo termina com código 0 sem fazer requisições, para que o
workflow não falhe.

Status: NÃO VALIDADO contra a API real (tamanho de página, limite de requisições
e a data usada pelo filtro). Validar na primeira execução com chave.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger("etl_discricionarias.portal")

BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados/convenios"
CHAVE_ENV = "PORTAL_TRANSPARENCIA_KEY"
UF = "TO"
DATA_INICIAL_HISTORICO = date(2008, 1, 1)
DIAS_INCREMENTAIS = 30
SALVAR_A_CADA_PAGINAS = 50
MAX_TENTATIVAS = 4
ESPERA_BASE_SEGUNDOS = 3
TIMEOUT_SEGUNDOS = 60

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORE_PATH = PROJECT_ROOT / "dataset" / "portal_convenios_to.json"
OUTPUT_PATH = PROJECT_ROOT / "data_discricionarias" / "processados" / "convenios_portal_to.parquet"


def _chave() -> str | None:
    chave = os.getenv(CHAVE_ENV, "").strip()
    return chave or None


def _get(params: dict, chave: str) -> list[dict]:
    """GET com retry. Nunca registra a chave em log."""
    ultimo_erro: Exception | None = None
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            response = requests.get(
                BASE_URL,
                params=params,
                headers={"chave-api-dados": chave, "Accept": "application/json"},
                timeout=TIMEOUT_SEGUNDOS,
            )
            if response.status_code == 429:
                espera = ESPERA_BASE_SEGUNDOS * tentativa * 2
                logger.warning("[portal] limite de requisições (429); aguardando %ss", espera)
                time.sleep(espera)
                continue
            response.raise_for_status()
            return response.json() or []
        except (requests.RequestException, ValueError) as erro:
            ultimo_erro = erro
            logger.warning("[portal] tentativa %s/%s falhou: %s", tentativa, MAX_TENTATIVAS,
                           type(erro).__name__)
            time.sleep(ESPERA_BASE_SEGUNDOS * tentativa)
    raise RuntimeError(f"[portal] falha após {MAX_TENTATIVAS} tentativas: {type(ultimo_erro).__name__}")


def _achatar(convenio: dict) -> dict:
    """Converte o ConvenioDTO aninhado em uma linha plana (nomes conferidos no openapi.json)."""
    dim = convenio.get("dimConvenio") or {}
    convenente = convenio.get("convenente") or {}
    municipio = convenio.get("municipioConvenente") or {}
    orgao = convenio.get("orgao") or {}
    tipo = convenio.get("tipoInstrumento") or {}
    return {
        "id_portal": convenio.get("id"),
        # `codigo` é o número do SICONV, que bate com nr_convenio do Transferegov.
        # `numero` é texto livre ("CV 832/02"), usado só como referência.
        "nr_convenio_portal": dim.get("codigo"),
        "numero_portal": dim.get("numero"),
        "situacao_portal": convenio.get("situacao"),
        "data_inicio_vigencia": convenio.get("dataInicioVigencia"),
        "data_final_vigencia": convenio.get("dataFinalVigencia"),
        "data_conclusao": convenio.get("dataConclusao"),
        "data_ultima_liberacao": convenio.get("dataUltimaLiberacao"),
        "convenente_portal": convenente.get("nome"),
        "municipio_portal": municipio.get("nomeIBGE"),
        "orgao_portal": orgao.get("nome"),
        "tipo_instrumento_portal": tipo.get("descricao"),
        "valor_portal": convenio.get("valor"),
        "valor_liberado_portal": convenio.get("valorLiberado"),
        "valor_contrapartida_portal": convenio.get("valorContrapartida"),
        "valor_ultima_liberacao_portal": convenio.get("valorDaUltimaLiberacao"),
    }


def _carregar_store() -> dict:
    if STORE_PATH.exists():
        return json.loads(STORE_PATH.read_text(encoding="utf-8"))
    return {"backfill_completo": False, "ultima_pagina_backfill": 0, "convenios": {}}


def _salvar_store(store: dict) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(store, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STORE_PATH)  # troca atômica: uma interrupção não corrompe o progresso salvo


def _paginar(params: dict, chave: str, pagina_inicial: int, store: dict, ao_pagina=None) -> int:
    """Percorre as páginas até a primeira vazia. Retorna a última página lida."""
    pagina = pagina_inicial
    while True:
        dados = _get({**params, "pagina": pagina}, chave)
        if not dados:
            return pagina - 1
        for item in dados:
            if item.get("id") is not None:
                store["convenios"][str(item["id"])] = _achatar(item)
        if ao_pagina and pagina % SALVAR_A_CADA_PAGINAS == 0:
            ao_pagina(pagina)
        pagina += 1


def _backfill(chave: str, store: dict) -> None:
    """Lista todos os convênios de TO (filtro só por UF; a API aceita sem data). Retomável."""
    def checkpoint(pagina: int) -> None:
        store["ultima_pagina_backfill"] = pagina
        _salvar_store(store)
        logger.info("[portal] backfill página %s | convênios TO=%s", pagina, f"{len(store['convenios']):,}")

    inicio = store.get("ultima_pagina_backfill", 0) + 1
    ultima = _paginar({"uf": UF}, chave, inicio, store, ao_pagina=checkpoint)
    store["ultima_pagina_backfill"] = ultima
    store["backfill_completo"] = True
    _salvar_store(store)
    logger.info("[portal] backfill concluído | páginas=%s | convênios TO=%s", ultima, f"{len(store['convenios']):,}")


def _incremental(chave: str, store: dict) -> None:
    """Reconsulta convênios atualizados nos últimos DIAS_INCREMENTAIS dias (limite da API: até 1 mês)."""
    hoje = date.today()
    inicio = hoje - timedelta(days=DIAS_INCREMENTAIS - 1)
    params = {"uf": UF, "dataInicial": inicio.strftime("%d/%m/%Y"), "dataFinal": hoje.strftime("%d/%m/%Y")}
    ultima = _paginar(params, chave, 1, store)
    _salvar_store(store)
    logger.info("[portal] incremental %s→%s | páginas=%s | convênios TO=%s",
                inicio, hoje, ultima, f"{len(store['convenios']):,}")


def executar(modo: str) -> int:
    chave = _chave()
    if chave is None:
        logger.warning("[portal] %s não definida; coleta do Portal ignorada.", CHAVE_ENV)
        return 0

    store = _carregar_store()
    if modo == "backfill":
        if store.get("backfill_completo"):
            logger.info("[portal] carga histórica já concluída; use --modo incremental.")
        else:
            _backfill(chave, store)
    else:
        if not store.get("backfill_completo"):
            # Sem a carga histórica, o parquet ficaria só com os últimos dias. Falha explícita.
            logger.error("[portal] carga histórica incompleta. "
                         "Rode 'python backend/coletor_portal_convenios.py --modo backfill' antes.")
            return 1
        _incremental(chave, store)

    df = pd.DataFrame(list(store["convenios"].values()))
    if df.empty:
        logger.warning("[portal] nenhum convênio de TO na base; parquet não gerado.")
        return 0
    df["nr_convenio_portal"] = df["nr_convenio_portal"].astype("string").str.strip()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False, compression="snappy")
    logger.info("[portal] parquet salvo | linhas=%s | caminho=%s", f"{len(df):,}", OUTPUT_PATH)
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--modo", choices=["backfill", "incremental"], default="incremental")
    return executar(parser.parse_args().modo)


if __name__ == "__main__":
    raise SystemExit(main())
