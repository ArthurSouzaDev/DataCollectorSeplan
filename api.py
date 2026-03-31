import requests
import os
from dotenv import load_dotenv
import csv
import pandas as pd
from datetime import datetime

load_dotenv()

fundo_a_fundo = os.getenv("URL_fundo_a_fundo")
transf_especial = os.getenv("URL_trans_especial")

def extrair_dados(endpoint, params):
    todos_os_dados = []
    limit = 1000
    offset = 0

    while True:
        params['limit'] = limit
        params['offset'] = offset

        response = requests.get(endpoint, params=params)

        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            break

        dados = response.json()

        if not dados:
            break

        todos_os_dados.extend(dados)
        print(f"Página offset {offset} — {len(dados)} registros recebidos")

        if len(dados) < limit:
            break

        offset += limit

    print(f"Total extraído: {len(todos_os_dados)} registros")
    return todos_os_dados
params_fundo = {
    'uf_ente_recebedor_plano_acao': 'eq.TO',
}
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
    
def tratar_dados(dados):
    dados_tratados = []

    for item in dados:
        uf = (item.get("uf_ente_recebedor_plano_acao")or "").strip().upper()

        if uf != "TO":
            continue

        registro = {
            "codigo_plano": item.get("codigo_plano_acao"),
            "situacao": item.get("situacao_plano_acao") or "Não informado",
            "data_inicio": tratar_data(item.get("data_inicio_vigencia_plano_acao")),
            "data_fim": tratar_data(item.get("data_fim_vigencia_plano_acao")),
            "ente_recebedor": item.get("nome_ente_recebedor_plano_acao") or "Não informado",
            "municipio": item.get("nome_municipio_ente_recebedor_plano_acao") or "Não informado",
            "uf": uf,
            "cnpj_recebedor": item.get("cnpj_ente_recebedor_plano_acao") or "Não informado",
            "fundo_repassador": item.get("nome_fundo_repassador_plano_acao") or "Não informado",
            "orgao_repassador": item.get("nome_orgao_repassador_plano_acao") or "Não informado",
            "sigla_orgao": item.get("sigla_orgao_repassador_plano_acao") or "Não informado",
            "fundo_recebedor": item.get("nome_fundo_recebedor_plano_acao") or "Não informado",
            "valor_emenda": tratar_float(item.get("valor_repasse_emenda_plano_acao")),
            "valor_especifico": tratar_float(item.get("valor_repasse_especifico_plano_acao")),
            "valor_voluntario": tratar_float(item.get("valor_repasse_voluntario_plano_acao")),
            "valor_total_repasse": tratar_float(item.get("valor_total_repasse_plano_acao")),
            "valor_total_plano": tratar_float(item.get("valor_total_plano_acao")),
            "saldo_disponivel": tratar_float(item.get("valor_saldo_disponivel_plano_acao")),
            }
        dados_tratados.append(registro)

    return dados_tratados
dados = extrair_dados(fundo_a_fundo,params_fundo)
dados_tratados = tratar_dados(dados)

print(f"Total extraído: {len(dados)}")
print(f"Total tratado: {len(dados_tratados)}")

df = pd.DataFrame(dados_tratados)
print(df[["data_inicio", "data_fim"]].head(10))
df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
df["data_fim"] = pd.to_datetime(df["data_fim"], errors="coerce")

df.to_csv("tocantins_bi.csv", index=False, sep=";", date_format="%Y-%m-%d")
