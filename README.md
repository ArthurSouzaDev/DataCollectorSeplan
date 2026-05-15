# DataCollectorSeplan

Sistema desenvolvido para coleta e consolidação de dados públicos utilizando APIs governamentais.

O projeto automatiza a extração de informações relacionadas a transferências, emendas e recursos públicos, permitindo transformar dados brutos em arquivos estruturados para análise.

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
├── api.py                         # Integrações e chamadas para APIs
├── app.py                         # Aplicação principal
├── app_discricionarias.py         # Execução específica para dados discricionários
├── coletor_discricionarias.py     # Lógica de coleta de dados discricionários
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

---

## Configuração

O projeto utiliza variáveis de ambiente através do arquivo `.env`.

Exemplo:

```env
TOKEN=seu_token
URL_API=sua_url
```

---

## Execução

Para gerar os dados discricionários processados:

```bash
python coletor_discricionarias.py
```

Para executar a aplicação principal:

```bash
streamlit run app.py
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
* Agendamento automático de coletas
* Containerização com Docker
* API própria para consulta dos dados consolidados

---

## Autor

Arthur Souza

* GitHub: [https://github.com/ArthurSouzaDev](https://github.com/ArthurSouzaDev)
* Repositório: [https://github.com/ArthurSouzaDev/DataCollectorSeplan](https://github.com/ArthurSouzaDev/DataCollectorSeplan)
