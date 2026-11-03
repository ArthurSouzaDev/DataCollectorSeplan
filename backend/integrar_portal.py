"""Integra os convênios do Portal da Transparência ao parquet de discricionárias.

Junção: nr_convenio (Transferegov, número SICONV) = dimConvenio.codigo (Portal).

Campos acrescentados (ou atualizados, se já existirem):
- situacao_convenio: situação do convênio no Portal (ex.: "EM EXECUÇÃO").
  NÃO substitui `situacao`, que continua sendo o grupo da proposta, como no painel da SERPRO.
- valor_desembolsado: valor liberado do convênio (Portal). Alimenta "Valor liberado" e o funil.
- data_final_vigencia, data_ultima_liberacao: datas do convênio no Portal.

Campos que o Portal não traz (valor pago, saldo em conta, valores devolvidos) continuam zerados.

Roda depois do coletor do Portal. Sem o arquivo do Portal, não faz nada.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("etl_discricionarias.integracao")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSADOS = PROJECT_ROOT / "data_discricionarias" / "processados"
BASE_PATH = PROCESSADOS / "discricionarias_to.parquet"
PORTAL_PATH = PROCESSADOS / "convenios_portal_to.parquet"

COLUNAS_PORTAL = ["situacao_convenio", "valor_desembolsado", "data_final_vigencia", "data_ultima_liberacao"]


def integrar(base: pd.DataFrame, portal: pd.DataFrame) -> pd.DataFrame:
    base = base.drop(columns=[c for c in COLUNAS_PORTAL if c in base.columns])

    portal = portal[portal["nr_convenio_portal"].astype("string").str.strip().ne("")].copy()
    portal["nr_convenio_portal"] = portal["nr_convenio_portal"].astype("string").str.strip()
    # Um código de convênio pode aparecer mais de uma vez no Portal: fica o registro mais recente.
    portal = portal.sort_values("data_ultima_liberacao", na_position="first").drop_duplicates(
        subset="nr_convenio_portal", keep="last"
    )

    referencia = pd.DataFrame(
        {
            "nr_convenio": portal["nr_convenio_portal"],
            "situacao_convenio": portal["situacao_portal"].astype("string").fillna("Não informado"),
            "valor_desembolsado": pd.to_numeric(portal["valor_liberado_portal"], errors="coerce").fillna(0.0),
            "data_final_vigencia": pd.to_datetime(portal["data_final_vigencia"], errors="coerce"),
            "data_ultima_liberacao": pd.to_datetime(portal["data_ultima_liberacao"], errors="coerce"),
        }
    )

    base["nr_convenio"] = base["nr_convenio"].astype("string").str.strip()
    resultado = base.merge(referencia, on="nr_convenio", how="left")
    sem_numero = resultado["nr_convenio"].isna()
    resultado.loc[sem_numero, "situacao_convenio"] = "Não se aplica (proposta)"
    resultado["situacao_convenio"] = resultado["situacao_convenio"].fillna("Sem correspondência no Portal")
    resultado["valor_desembolsado"] = pd.to_numeric(resultado["valor_desembolsado"], errors="coerce").fillna(0.0)
    return resultado


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    if not PORTAL_PATH.exists():
        logger.warning("[integracao] %s não existe; nada a integrar.", PORTAL_PATH.name)
        return 0
    if not BASE_PATH.exists():
        logger.error("[integracao] base de discricionárias ausente; rode o coletor antes.")
        return 1

    base = pd.read_parquet(BASE_PATH)
    portal = pd.read_parquet(PORTAL_PATH)
    resultado = integrar(base, portal)

    com_portal = int((resultado["nr_convenio"].notna() & resultado["situacao_convenio"].ne("Sem correspondência no Portal")).sum())
    convenios = int(resultado["fase"].eq("Convênio").sum()) if "fase" in resultado else 0
    logger.info("[integracao] linhas=%s | com convênio no Portal=%s | convênios (fase)=%s",
                f"{len(resultado):,}", f"{com_portal:,}", f"{convenios:,}")

    resultado.to_parquet(BASE_PATH, index=False, compression="snappy")
    logger.info("[integracao] parquet atualizado: %s", BASE_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
