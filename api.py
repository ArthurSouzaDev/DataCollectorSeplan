import requests
import os
from dotenv import load_dotenv
load_dotenv()
fundo_a_fundo = os.getenv("URL_fundo_a_fundo")
transf_especial = os.getenv("URL_trans_especial")

paramsFund = {
    'limit':2,
    'uf_ente_recebedor_plano_acao':'eq.TO', #Poderia ser adicionado um OR, porém não tem necessidade
    # 'data_inicio_vigencia_plano_acao':'gte.2026.01.01', #opção de colocar ano
    # 'situacao_plano_acao':'eq.EM_ELABORACAO'
    # 'nome_ente_recebedor_plano_acao':'all.FME'eq.TO' or 
}



paramsEspecial = {

}

response = requests.get(fundo_a_fundo,paramsFund)
dados = response.json()
count = 0

# if response.status_code == 200:
#     dados = response.json()
#     for item in dados:
#         count+=1
#         print(item)        
# else:   
#     print("Erro:", response.status_code)    
print(f"São {count} planos encontrados")
print(dados)
