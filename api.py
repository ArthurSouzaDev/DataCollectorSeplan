import requests
import os
from dotenv import load_dotenv
import json
load_dotenv()
fundo_a_fundo = os.getenv("URL_fundo_a_fundo")
transf_especial = os.getenv("URL_trans_especial")
paramsFund = {
    'uf_ente_recebedor_plano_acao':'eq.TO',

    }

def extract_data(endpoint,params):
    response = requests.get(endpoint,params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao extrair dados da API: {response.status_code}")            
        return None

print(extract_data(fundo_a_fundo,paramsFund))



