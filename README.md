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
* Exportação em CSV
* Utilização de cache local para otimização
* Organização de dados para análise posterior

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

Para executar a aplicação principal:

```bash
python app.py
```

Para executar a coleta de dados discricionários:

```bash
python app_discricionarias.py
```

---

## Fluxo da aplicação

1. O sistema realiza requisições para APIs governamentais
2. Os dados públicos são coletados automaticamente
3. As informações são tratadas e organizadas
4. Os resultados são exportados em CSV
5. Os arquivos podem ser utilizados para análise e consolidação de dados

---

## Observações

A pasta relacionada aos dados discricionários pode conter arquivos muito grandes e, por esse motivo, nem todos os dados brutos foram adicionados ao repositório.

O projeto mantém apenas os arquivos essenciais para execução e demonstração da estrutura da aplicação.

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
