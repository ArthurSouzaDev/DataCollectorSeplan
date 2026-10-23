# DataCollectorSeplan

Sistema desenvolvido para coleta e consolidação de dados públicos utilizando APIs governamentais.

O projeto automatiza a extração de informações relacionadas a transferências, emendas e recursos públicos, permitindo transformar dados brutos em arquivos estruturados para análise.

Dados disponibilizados em: [https://api-publica.transferegov.gestao.gov.br/](https://api-publica.transferegov.gestao.gov.br/)

---

## Objetivo

O DataCollectorSeplan foi criado para reduzir o trabalho manual de coleta de dados públicos, automatizando consultas em APIs do Governo e organizando os resultados em arquivos CSV.

O foco principal do projeto é:

* Automatizar consultas em APIs governamentais
* Consolidar dados públicos em bases estruturadas
* Facilitar análises posteriores
* Reduzir retrabalho operacional
* Centralizar informações em um único fluxo de coleta

---

## Tecnologias utilizadas

* Python
* Requests
* Pandas
* APIs REST Governamentais
* CSV
* JSON
* Variáveis de ambiente com `.env`

---

## Estrutura do projeto

```bash
DataCollectorSeplan/
├── backend/
│   ├── api.py                     # Coleta fundo a fundo e transferências especiais (Transferegov)
│   └── coletor_discricionarias.py # Lógica de coleta de dados discricionários
├── Legacy/                        # Interface Streamlit antiga (substituída pelo frontend React)
│   ├── app.py                     # Aplicação principal Streamlit
│   └── app_discricionarias.py     # Aba para dados discricionários
├── dataset/
│   ├── cache_natureza.json
│   ├── emendas_to.csv
│   └── fundo_a_fundo.csv
├── frontend/                      # Estrutura base React/TypeScript
├── requirements.txt               # Dependências do projeto
├── .env                           # Variáveis de ambiente
└── README.md
```

---

## Funcionalidades

* Consumo de APIs públicas do Governo
* Coleta automatizada de dados
* Consolidação de informações públicas
* Processamento em chunks para reduzir uso de memória
* Exportação final em Parquet compactado
* Separação entre ETL e interface Streamlit

---

## Pré-requisitos

Antes de executar o projeto, é necessário possuir:

* Python 3.10+
* Pip

---

## Instalação

Clone o repositório:

```bash
git clone https://github.com/ArthurSouzaDev/DataCollectorSeplan.git
```

Acesse a pasta do projeto:

```bash
cd DataCollectorSeplan
```

Crie um ambiente virtual:

### Windows

```bash
python -m venv venv
venv\Scripts\activate
```

### Linux/macOS

```bash
python3 -m venv venv
source venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

### Dependências para rodar apenas a API (`backend/api.py`)

Se quiser instalar somente o necessário para a coleta via API:

```bash
pip install requests pandas python-dotenv
```

| Pacote          | Uso no `api.py`                                     |
| --------------- | --------------------------------------------------- |
| `requests`      | Requisições à API do Transferegov e à BrasilAPI     |
| `pandas`        | Montagem dos DataFrames e exportação dos CSVs       |
| `python-dotenv` | Leitura das URLs configuradas no arquivo `.env`     |

Os demais módulos usados (`os`, `sys`, `json`, `time`, `datetime`, `pathlib`) já vêm com o Python.

> **Atenção:** se estiver usando o ambiente virtual, ative-o antes do `pip install`. Caso contrário, os pacotes são instalados no Python global e a execução dentro do `venv` falha com `ModuleNotFoundError: No module named 'dotenv'`.

---

## Configuração

O projeto utiliza variáveis de ambiente através do arquivo `.env` na raiz do projeto:

```env
URL_FUNDO_A_FUNDO="https://api-publica.transferegov.gestao.gov.br/fundoafundo/planos-acao"
URL_TRANSF_ESPECIAL="https://api-publica.transferegov.gestao.gov.br/especiais/planos-acao-especiais"
```

No GitHub Actions, essas mesmas variáveis são lidas dos *secrets* do repositório.

---

## Mudanças na API do Transferegov (outubro/2026)

A API pública do Transferegov mudou de formato, e a coleta do `api.py` passou a falhar. O código foi ajustado para o formato novo:

* **Filtro de UF:** o formato `eq.TO` deixou de ser aceito (a API retornava HTTP 422). Agora o valor é passado diretamente: `TO`.
* **Paginação:** os parâmetros `limit`/`offset` foram substituídos por `pagina`/`tamanho_da_pagina` (máximo de 1000 registros por página no fundo a fundo e 200 nas transferências especiais).
* **Formato da resposta:** os registros agora vêm dentro de `{"data": [...], "total_pages": ..., "total_items": ...}`, e não mais como uma lista direta.
* **Transferências especiais (emendas):** o endpoint `/especiais/planos-acao-especiais` não filtra mais por UF e não traz nome, CNPJ e UF do beneficiário. A coleta agora:
  1. busca os beneficiários do TO em `/especiais/beneficiarios-especiais`;
  2. consulta os planos de ação de cada beneficiário;
  3. completa cada plano com os dados do beneficiário.

  Isso faz cerca de 140 requisições por execução, então a etapa leva alguns minutos.

Os CSVs gerados (`dataset/fundo_a_fundo.csv` e `dataset/emendas_to.csv`) **mantêm as mesmas colunas** de antes. A primeira coleta no formato novo foi comparada com a última coleta salva: todos os planos antigos continuam presentes, sem duplicidade e com os mesmos valores, além de novos planos publicados no período.

O `api.py` monta os endereços de planos e beneficiários a partir da base de `URL_TRANSF_ESPECIAL`. Por isso, a URL antiga (`/especiais/programas-especiais`) também continua funcionando.

---

## Execução

Para coletar os dados de fundo a fundo e transferências especiais:

```bash
python backend/api.py
```

Para gerar os dados discricionários processados:

```bash
python backend/coletor_discricionarias.py
```

Para executar a interface Streamlit antiga (legado):

```bash
streamlit run Legacy/app.py
```

---

## Fluxo dos dados discricionários

```text
ETL
↓
Download atualizado dos ZIPs GOV
↓
Extração dos CSVs
↓
Tratamento em chunks
↓
Parquet em data_discricionarias/processados/
↓
Streamlit lê apenas o parquet pronto
```

---

## Observações

A pasta `data_discricionarias/` é organizada em:

```text
data_discricionarias/
├── cache_bruto/   # ZIPs baixados a cada execução do ETL
├── extraidos/     # CSVs temporários extraídos dos ZIPs
└── processados/   # Parquet final consumido pelo Streamlit
```

Os arquivos brutos e extraídos são ignorados pelo Git. O artefato final leve em Parquet pode ser versionado e é atualizado automaticamente pelo workflow do GitHub Actions a cada 12 horas.

---

## Melhorias futuras

* Integração com banco de dados
* Containerização com Docker
* API própria para consulta dos dados consolidados

---

## Autor

Arthur Souza

* GitHub: [https://github.com/ArthurSouzaDev](https://github.com/ArthurSouzaDev)
* Repositório: [https://github.com/ArthurSouzaDev/DataCollectorSeplan](https://github.com/ArthurSouzaDev/DataCollectorSeplan)
* Site: [https://data-collector-seplan.vercel.app/](https://data-collector-seplan.vercel.app/)
