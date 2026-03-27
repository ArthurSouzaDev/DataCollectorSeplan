import requests
import os
from dotenv import load_dotenv
load_dotenv()
fundo_a_fundo = os.getenv("URL_fundo_a_fundo")

params = {
    # "ano_programa": "eq.2025"
    'limit':50,
    'uf_ente_recebedor_plano_acao':'eq.TO',
    'data_inicio_vigencia_plano_acao':'gte.2026.01.01'  
}

response = requests.get(fundo_a_fundo,params)
count = 0
if response.status_code == 200:
    dados = response.json()
    for item in dados:
        count+=1
        print(item)        
else:   
    print("Erro:", response.status_code)    
print(f"São {count} planos encontrados")
