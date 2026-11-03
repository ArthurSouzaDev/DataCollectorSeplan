"""Compara a base anterior com a atual e registra o resumo no histórico.

Uso:
  python scripts/comparar_execucoes.py --anterior ant.csv --atual atual.csv --chave codigo_plano \
      --titulo "Fundo a fundo" --historico public/data/historico.json --dataset-id fundo-a-fundo

- Sem --historico: imprime só o Markdown (usado no Job Summary).
- Com --historico: também acrescenta uma entrada em public/data/historico.json e
  remove entradas com mais de RETENCAO_DIAS dias.

Aceita .parquet e .csv (separador ';'). Sempre sai com código 0: é informativo e
não deve derrubar a rotina de atualização.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

MAX_EXEMPLOS = 15
RETENCAO_DIAS = 90
FUSO = timezone(timedelta(hours=-3))  # America/Araguaina (Tocantins, sem horário de verão)


def carregar(caminho: str) -> pd.DataFrame:
    path = Path(caminho)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str, keep_default_na=False)


def _normalizar(serie: pd.Series) -> pd.Series:
    return serie.astype("string").fillna("").str.strip()


def _totais(df: pd.DataFrame) -> dict[str, float]:
    return {
        col: float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
        for col in df.columns
        if col.startswith(("valor_", "vl_"))
    }


def resumo(antes: pd.DataFrame, depois: pd.DataFrame, chave: str) -> dict:
    """Calcula o resumo estruturado. Os mesmos dados alimentam o Markdown e o histórico."""
    if chave not in antes.columns or chave not in depois.columns:
        return {"erro": f"chave '{chave}' ausente em uma das bases"}

    antes = antes.drop_duplicates(subset=chave, keep="last").set_index(chave)
    depois = depois.drop_duplicates(subset=chave, keep="last").set_index(chave)
    novos = depois.index.difference(antes.index)
    removidos = antes.index.difference(depois.index)
    comuns = antes.index.intersection(depois.index)

    campos = []
    for col in [c for c in depois.columns if c in antes.columns]:
        if len(comuns) == 0:
            break
        diferentes = int((_normalizar(antes.loc[comuns, col]) != _normalizar(depois.loc[comuns, col])).sum())
        if diferentes:
            campos.append({"campo": col, "registros": diferentes})
    campos.sort(key=lambda x: -x["registros"])

    ta, tb = _totais(antes), _totais(depois)
    totais = [{"campo": c, "antes": ta[c], "depois": tb[c]} for c in tb if c in ta and (ta[c] or tb[c])]

    situacoes = []
    situacoes_novas: list[str] = []
    if "situacao" in antes.columns and "situacao" in depois.columns:
        ca = _normalizar(antes["situacao"]).value_counts()
        cb = _normalizar(depois["situacao"]).value_counts()
        for s in sorted(set(ca.index) | set(cb.index), key=lambda s: -int(cb.get(s, 0))):
            situacoes.append({"situacao": s or "(vazio)", "antes": int(ca.get(s, 0)), "depois": int(cb.get(s, 0))})
        situacoes_novas = [s for s in cb.index if s not in ca.index and s]

    return {
        "registros": {"antes": len(antes), "depois": len(depois)},
        "novos": {"total": len(novos), "exemplos": [str(x) for x in novos[:MAX_EXEMPLOS]]},
        "removidos": {"total": len(removidos), "exemplos": [str(x) for x in removidos[:MAX_EXEMPLOS]]},
        "colunas_novas": [c for c in depois.columns if c not in antes.columns],
        "colunas_removidas": [c for c in antes.columns if c not in depois.columns],
        "campos_alterados": campos,
        "totais": totais,
        "situacoes": situacoes,
        "situacoes_novas": situacoes_novas,
    }


def markdown(res: dict, titulo: str) -> str:
    linhas = [f"## {titulo}", ""]
    if "erro" in res:
        return "\n".join(linhas + [res["erro"]]) + "\n"

    r = res["registros"]
    linhas += ["### Volume", "", "| | Anterior | Atual | Diferença |", "|---|---:|---:|---:|",
               f"| Registros | {r['antes']:,} | {r['depois']:,} | {r['depois'] - r['antes']:+,} |", "",
               f"- Novos registros: **{res['novos']['total']:,}**",
               f"- Registros removidos: **{res['removidos']['total']:,}**"]
    if res["novos"]["exemplos"]:
        linhas.append(f"  - exemplos: {', '.join(res['novos']['exemplos'])}")
    if res["removidos"]["exemplos"]:
        linhas.append(f"  - exemplos: {', '.join(res['removidos']['exemplos'])}")

    linhas += ["", "### Estrutura", "",
               f"- Colunas novas: {', '.join(res['colunas_novas']) or 'nenhuma'}",
               f"- Colunas removidas: {', '.join(res['colunas_removidas']) or 'nenhuma'}"]

    if res["campos_alterados"]:
        linhas += ["", "### Campos alterados (registros em comum)", "",
                   "| Campo | Registros alterados |", "|---|---:|"]
        linhas += [f"| `{c['campo']}` | {c['registros']:,} |" for c in res["campos_alterados"][:MAX_EXEMPLOS]]

    if res["totais"]:
        linhas += ["", "### Totais monetários", "", "| Campo | Anterior | Atual | Variação |", "|---|---:|---:|---:|"]
        linhas += [f"| `{t['campo']}` | {t['antes']:,.2f} | {t['depois']:,.2f} | {t['depois'] - t['antes']:+,.2f} |"
                   for t in res["totais"][:MAX_EXEMPLOS]]

    if res["situacoes"]:
        linhas += ["", "### Situações", "", "| Situação | Anterior | Atual | Diferença |", "|---|---:|---:|---:|"]
        linhas += [f"| {s['situacao']} | {s['antes']:,} | {s['depois']:,} | {s['depois'] - s['antes']:+,} |"
                   for s in res["situacoes"][:MAX_EXEMPLOS]]
        if res["situacoes_novas"]:
            linhas += ["", f"Situações que apareceram pela primeira vez: {', '.join(res['situacoes_novas'])}"]

    return "\n".join(linhas) + "\n"


def registrar_historico(caminho: Path, dataset_id: str, res: dict, sem_base: bool) -> None:
    """Acrescenta a execução ao histórico e apaga entradas além da retenção."""
    agora = datetime.now(FUSO)
    historico = {"atualizado_em": None, "entradas": []}
    if caminho.exists():
        historico = json.loads(caminho.read_text(encoding="utf-8"))

    entrada = {"dataset": dataset_id, "executado_em": agora.isoformat(timespec="minutes"),
               "sem_base_anterior": sem_base, **res}
    entradas = historico.get("entradas", []) + [entrada]

    limite = agora - timedelta(days=RETENCAO_DIAS)
    entradas = [e for e in entradas if datetime.fromisoformat(e["executado_em"]) >= limite]

    historico = {"atualizado_em": agora.isoformat(timespec="minutes"), "entradas": entradas}
    caminho.parent.mkdir(parents=True, exist_ok=True)
    tmp = caminho.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(historico, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(caminho)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--anterior", required=True)
    parser.add_argument("--atual", required=True)
    parser.add_argument("--chave", required=True)
    parser.add_argument("--titulo", default="Comparação com a execução anterior")
    parser.add_argument("--historico", help="caminho do historico.json a atualizar")
    parser.add_argument("--dataset-id", help="id do dataset no histórico (ex.: fundo-a-fundo)")
    args = parser.parse_args()

    atual = carregar(args.atual)
    try:
        anterior = carregar(args.anterior)
        sem_base = False
        res = resumo(anterior, atual, args.chave)
    except Exception as erro:  # primeira execução ou arquivo ausente no commit anterior
        sem_base = True
        res = {"registros": {"antes": None, "depois": len(atual)}, "novos": {"total": 0, "exemplos": []},
               "removidos": {"total": 0, "exemplos": []}, "colunas_novas": [], "colunas_removidas": [],
               "campos_alterados": [], "totais": [], "situacoes": [], "situacoes_novas": []}
        print(f"## {args.titulo}\n\nSem base anterior para comparar ({type(erro).__name__}).")

    if not sem_base:
        print(markdown(res, args.titulo))

    if args.historico and args.dataset_id and "erro" not in res:
        registrar_historico(Path(args.historico), args.dataset_id, res, sem_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
