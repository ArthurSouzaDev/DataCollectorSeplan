"""Cliente HTTP da API de Transferências Especiais: retry, paginação e erros claros."""

from __future__ import annotations

import os
import time

import requests

BASE_URL = os.getenv(
    "API_ESPECIAIS_BASE_URL",
    "https://api-publica.transferegov.gestao.gov.br/especiais",
).rstrip("/")

TAMANHO_PAGINA = 200          # máximo aceito pela API (valor maior retorna HTTP 422)
MAX_TENTATIVAS = 3
TIMEOUT_SEGUNDOS = 60
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


class ErroApi(Exception):
    """Falha definitiva em uma consulta (erro 4xx ou esgotamento de tentativas)."""


def _get(caminho: str, params: dict) -> object:
    url = f"{BASE_URL}{caminho}"
    ultimo_erro: Exception | None = None

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            resposta = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT_SEGUNDOS)
            if resposta.status_code == 429 or resposta.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resposta.status_code}", response=resposta)
            if resposta.status_code >= 400:
                # Erro de cliente (parâmetro inválido etc.): repetir não resolve.
                raise ErroApi(f"HTTP {resposta.status_code} em {caminho} {params}: {resposta.text[:300]}")
            return resposta.json()
        except (requests.RequestException, ValueError) as erro:
            ultimo_erro = erro
            if tentativa < MAX_TENTATIVAS:
                time.sleep(2 ** tentativa)

    raise ErroApi(f"Falha após {MAX_TENTATIVAS} tentativas em {caminho} {params}: {ultimo_erro}")


def buscar_todas_as_paginas(caminho: str, params: dict, paginado: bool = True) -> list[dict]:
    """Retorna todos os registros de uma consulta, percorrendo as páginas."""
    if not paginado:
        corpo = _get(caminho, dict(params))
        return [corpo] if isinstance(corpo, dict) and corpo else []

    registros: list[dict] = []
    pagina = 1
    while True:
        corpo = _get(caminho, {**params, "pagina": pagina, "tamanho_da_pagina": TAMANHO_PAGINA})
        dados = (corpo or {}).get("data") or []
        total_paginas = (corpo or {}).get("total_pages") or 1

        registros.extend(dados)
        if not dados or pagina >= total_paginas:
            break
        pagina += 1

    return registros
