# Relatorio de Reestruturacao Frontend e Organizacao do Projeto

Data: 04/06/2026

## Objetivo

Documentar as alteracoes realizadas para preparar o projeto para desenvolvimento frontend com React, TypeScript e Yarn, mantendo o backend Python organizado e reduzindo arquivos desnecessarios no controle de versao.

## Resumo das Alteracoes

Foram realizadas quatro frentes principais:

1. Organizacao da estrutura do projeto.
2. Configuracao inicial do React com TypeScript e Vite.
3. Padronizacao do uso do Yarn como gerenciador de pacotes.
4. Atualizacao de caminhos, workflow e arquivos ignorados pelo Git.

## Estrutura Atual

```text
DataCollectorSeplan/
├── backend/
│   ├── api.py
│   ├── app.py
│   ├── app_discricionarias.py
│   └── coletor_discricionarias.py
├── dataset/
│   ├── cache_natureza.json
│   ├── emendas_to.csv
│   └── fundo_a_fundo.csv
├── frontend/
│   ├── public/
│   └── src/
│       ├── app/
│       │   ├── providers/
│       │   └── routes/
│       ├── assets/
│       ├── components/
│       │   ├── layout/
│       │   └── ui/
│       ├── features/
│       ├── hooks/
│       ├── lib/
│       ├── pages/
│       ├── services/
│       │   └── api/
│       ├── styles/
│       └── types/
├── data_discricionarias/
├── Doc/
├── index.html
├── package.json
├── tsconfig.json
├── tsconfig.app.json
├── tsconfig.node.json
├── vite.config.ts
├── yarn.lock
└── .yarnrc.yml
```

## Backend

Os arquivos Python foram movidos da raiz para a pasta `backend/`:

```text
api.py                         -> backend/api.py
app.py                         -> backend/app.py
app_discricionarias.py         -> backend/app_discricionarias.py
coletor_discricionarias.py     -> backend/coletor_discricionarias.py
```

### Ajustes de Caminho

O arquivo `backend/api.py` passou a gravar os arquivos consolidados em `dataset/`:

```text
dataset/cache_natureza.json
dataset/emendas_to.csv
dataset/fundo_a_fundo.csv
```

O arquivo `backend/app.py` passou a ler os CSVs a partir de `dataset/`.

O arquivo `backend/app_discricionarias.py` continua lendo o Parquet processado em:

```text
data_discricionarias/processados/discricionarias_to.parquet
```

O arquivo `backend/coletor_discricionarias.py` continua usando `data_discricionarias/` na raiz do projeto para cache, extraidos e processados.

## Dataset

Os arquivos de base foram movidos da raiz para `dataset/`:

```text
cache_natureza.json
emendas_to.csv
fundo_a_fundo.csv
```

Esses arquivos nao foram adicionados ao `.gitignore`, pois fazem parte da base consumida pela aplicacao e pelo workflow. Ignorar `dataset/` poderia quebrar um clone limpo do projeto caso os dados ainda nao tivessem sido gerados.

## Frontend

Foi criada a estrutura inicial para desenvolvimento em React com TypeScript.

Arquivos principais criados:

```text
index.html
vite.config.ts
tsconfig.json
tsconfig.app.json
tsconfig.node.json
frontend/src/main.tsx
frontend/src/app/App.tsx
frontend/src/app/providers/AppProviders.tsx
frontend/src/app/routes/router.tsx
frontend/src/components/layout/AppLayout.tsx
frontend/src/pages/HomePage.tsx
frontend/src/pages/NotFoundPage.tsx
frontend/src/services/api/client.ts
frontend/src/styles/global.css
frontend/src/vite-env.d.ts
```

### Roteamento

O roteamento inicial foi configurado em:

```text
frontend/src/app/routes/router.tsx
```

Rotas iniciais:

```text
/    -> HomePage
*    -> NotFoundPage
```

### Comunicacao com API

Foi criado um cliente HTTP base em:

```text
frontend/src/services/api/client.ts
```

Ele usa `fetch` e permite configurar a URL base via variavel de ambiente do Vite:

```text
VITE_API_BASE_URL
```

## Yarn

O projeto foi padronizado para usar Yarn 4:

```json
"packageManager": "yarn@4.16.0"
```

Foi criado o arquivo:

```text
.yarnrc.yml
```

Configuracao aplicada:

```yaml
nodeLinker: node-modules
```

Essa escolha evita o uso de Plug'n'Play neste momento, simplificando a compatibilidade com Vite, React e ferramentas comuns do ecossistema frontend.

### Arquivos Removidos

Foram removidos artefatos desnecessarios do npm e do Yarn PnP:

```text
package-lock.json
.pnp.cjs
.pnp.loader.mjs
.yarn/
```

`node_modules/` pode existir localmente apos `yarn install`, mas fica ignorado pelo Git.

## Dependencias Frontend

Dependencias adicionadas:

```text
react
react-dom
react-router-dom
```

Dependencias de desenvolvimento:

```text
@types/react
@types/react-dom
@vitejs/plugin-react
typescript
vite
eslint
prettier
```

## Scripts Disponiveis

Scripts adicionados ao `package.json`:

```bash
yarn dev
yarn build
yarn preview
yarn typecheck
```

Uso esperado:

```bash
yarn dev
```

Servidor local padrao:

```text
http://127.0.0.1:5173/
```

## Gitignore

O `.gitignore` foi atualizado para ignorar artefatos locais e de build:

```text
node_modules/
.pnp.*
.yarn/*
dist/
build/
vite-dev.log
__pycache__/
*.py[cod]
```

### Sobre `.gitkeep`

Arquivos `.gitkeep` foram mantidos em pastas ainda vazias do frontend para que o Git preserve a estrutura inicial.

Eles podem ser removidos quando a pasta passar a conter arquivos reais.

Nao e recomendado adicionar `.gitkeep` ao `.gitignore`, pois isso faria com que pastas vazias planejadas deixassem de aparecer em clones futuros.

## GitHub Actions

O workflow `.github/workflows/atualizar_dados.yml` foi ajustado para executar os scripts a partir da nova pasta `backend/`:

```bash
python backend/api.py
python backend/coletor_discricionarias.py
```

O `git add` do workflow tambem foi atualizado para versionar os arquivos em `dataset/`:

```bash
git add dataset/fundo_a_fundo.csv dataset/emendas_to.csv dataset/cache_natureza.json data_discricionarias/processados/discricionarias_to.parquet
```

## Comandos Atualizados

Executar ETL discricionarias:

```bash
python backend/coletor_discricionarias.py
```

Executar dashboard Streamlit:

```bash
streamlit run backend/app.py
```

Executar frontend React:

```bash
yarn dev
```

Gerar build frontend:

```bash
yarn build
```

Validar TypeScript:

```bash
yarn typecheck
```

## Validacoes Realizadas

Validacao de sintaxe Python:

```bash
python -m py_compile backend\api.py backend\app.py backend\app_discricionarias.py backend\coletor_discricionarias.py
```

Validacao TypeScript:

```bash
yarn typecheck
```

Build frontend:

```bash
yarn build
```

Resultado: as validacoes foram concluidas com sucesso.

Observacao: em ambiente Windows com sandbox, o Vite/Rolldown pode exigir permissao elevada por erro `spawn EPERM`. Esse comportamento esta relacionado ao ambiente de execucao, nao necessariamente ao codigo do projeto.

## Impactos e Riscos

### Impactos

- Comandos antigos como `python api.py` e `streamlit run app.py` deixam de ser os comandos corretos.
- O backend agora deve ser executado a partir dos caminhos em `backend/`.
- Os CSVs base passam a ficar em `dataset/`.
- O frontend passa a ter uma entrada Vite na raiz do projeto.

### Riscos de Regressao

- Scripts externos, atalhos locais ou documentacoes antigas que chamem arquivos Python na raiz precisam ser atualizados.
- Qualquer codigo que assuma CSVs na raiz deve passar a buscar em `dataset/`.
- Se `dataset/` for ignorado no Git, clones limpos podem ficar sem os arquivos base esperados.
- Se `node_modules/` for versionado por engano, o repositorio pode ganhar centenas ou milhares de arquivos desnecessarios.

## Recomendacoes

1. Manter `yarn.lock` versionado.
2. Manter `package.json` e `.yarnrc.yml` versionados.
3. Nao versionar `node_modules/`, `.yarn/`, `dist/`, `build/`, `.pnp.*` ou caches Python.
4. Manter `dataset/` versionado enquanto a aplicacao depender desses arquivos para abrir em clone limpo.
5. Remover `.gitkeep` apenas quando as pastas tiverem arquivos reais.
6. Atualizar documentacoes futuras sempre que comandos de execucao mudarem.

