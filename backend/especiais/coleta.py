"""Coleta todos os endpoints do módulo de Transferências Especiais e grava um CSV por dataset.

Uso (a partir da raiz do projeto):
    python -m backend.especiais.coleta
    python -m backend.especiais.coleta --limite-beneficiarios 3   # teste rápido
    python -m backend.especiais.coleta --workers 6

Saída: dataset/transf_especiais/<dataset>.csv e _manifest.json.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from .cliente import BASE_URL, ErroApi, buscar_todas_as_paginas
from .endpoints import ENDPOINTS, Endpoint

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAIDA_PADRAO = PROJECT_ROOT / "dataset" / "transf_especiais"
WORKERS_PADRAO = 4


def _log(nome: str, mensagem: str) -> None:
    print(f"[{nome}] {mensagem}", flush=True)


def coletar_endpoint(
    endpoint: Endpoint,
    resultados: dict[str, pd.DataFrame],
    workers: int,
) -> tuple[list[dict], list[str]]:
    """Coleta um endpoint. Devolve (registros, mensagens_de_erro)."""
    if endpoint.dependencia is None:
        registros = buscar_todas_as_paginas(endpoint.caminho, endpoint.filtros, endpoint.paginado)
        return registros, []

    dep = endpoint.dependencia
    origem = resultados.get(dep.origem)
    if origem is None or dep.coluna_origem not in origem.columns:
        return [], [f"dataset de origem '{dep.origem}' sem a coluna '{dep.coluna_origem}'"]

    valores = origem[dep.coluna_origem].dropna().unique().tolist()
    _log(endpoint.nome, f"consultando {len(valores)} valores de {dep.parametro} (workers={workers})")

    def consultar(valor):
        registros = buscar_todas_as_paginas(
            endpoint.caminho, {**endpoint.filtros, dep.parametro: valor}, endpoint.paginado
        )
        for registro in registros:
            registro.setdefault(dep.parametro, valor)
        return registros

    registros: list[dict] = []
    erros: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futuros = [pool.submit(consultar, valor) for valor in valores]
        for concluidos, futuro in enumerate(as_completed(futuros), 1):
            try:
                registros.extend(futuro.result())
            except ErroApi as erro:
                erros.append(str(erro))
            if concluidos % 500 == 0:
                _log(endpoint.nome, f"{concluidos}/{len(valores)} consultas concluídas")

    return registros, erros


def _gravar_csv(df: pd.DataFrame, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".csv.tmp")
    df.to_csv(temporario, index=False, sep=";")
    temporario.replace(destino)  # só substitui o arquivo ao final de uma gravação completa


def executar(saida: Path, workers: int, limite_beneficiarios: int | None) -> int:
    resultados: dict[str, pd.DataFrame] = {}
    datasets_manifest: dict[str, dict] = {}
    total_erros = 0
    inicio_geral = time.time()

    for endpoint in ENDPOINTS:
        inicio = time.time()
        registros, erros = coletar_endpoint(endpoint, resultados, workers)

        df = pd.DataFrame(registros)
        if not df.empty:
            df = df.drop_duplicates()
        if endpoint.nome == "beneficiarios" and limite_beneficiarios:
            df = df.head(limite_beneficiarios)  # limita a árvore inteira, para testes

        resultados[endpoint.nome] = df
        _gravar_csv(df, saida / f"{endpoint.nome}.csv")

        total_erros += len(erros)
        datasets_manifest[endpoint.nome] = {
            "descricao": endpoint.descricao,
            "registros": len(df),
            "colunas": len(df.columns),
            "arquivo": f"{endpoint.nome}.csv",
            "erros": len(erros),
            "exemplos_erro": erros[:5],
            "segundos": round(time.time() - inicio, 1),
        }
        _log(endpoint.nome, f"{len(df):,} registros em {time.time() - inicio:.1f}s | erros: {len(erros)}")

    manifest = {
        "gerado_em": dt.datetime.now(dt.timezone(dt.timedelta(hours=-3))).isoformat(),
        "base_url": BASE_URL,
        "uf": "TO",
        "limite_beneficiarios": limite_beneficiarios,
        "segundos_total": round(time.time() - inicio_geral, 1),
        "datasets": datasets_manifest,
    }
    (saida / "_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nConcluído em {manifest['segundos_total']}s. Arquivos em: {saida}", flush=True)
    if total_erros:
        print(f"ATENÇÃO: {total_erros} consulta(s) falharam. Veja _manifest.json.", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Coleta o módulo de Transferências Especiais (TO).")
    parser.add_argument("--saida", type=Path, default=SAIDA_PADRAO, help="Pasta de saída dos CSVs")
    parser.add_argument("--workers", type=int, default=WORKERS_PADRAO, help="Consultas paralelas")
    parser.add_argument(
        "--limite-beneficiarios", type=int, default=None,
        help="Usa só os N primeiros beneficiários (teste rápido)",
    )
    args = parser.parse_args(argv)
    return executar(args.saida, args.workers, args.limite_beneficiarios)


if __name__ == "__main__":
    sys.exit(main())
