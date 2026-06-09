# Relatorio de Criacao do Dashboard Frontend React

Data: 09/06/2026

## Objetivo

Documentar as alteracoes realizadas no frontend React para criar uma primeira versao funcional do dashboard DataCollector Seplan, com pre-visualizacao dos principais conjuntos de dados, paginas detalhadas, filtros interativos, visualizacao de planilha, download de CSV e suporte a dark mode.

Este documento tambem registra as limitacoes atuais da implementacao, principalmente o fato de o frontend consumir dados locais estaticos enquanto a API backend ainda nao foi criada.

## Escopo da Alteracao

A alteracao foi limitada ao frontend React/Vite e aos arquivos publicos usados para prototipagem dos dados.

Nao foram alterados:

- arquivos `.env`;
- scripts Python de ETL;
- regras de coleta em `backend/api.py`;
- regras de coleta em `backend/coletor_discricionarias.py`;
- autenticacao;
- banco de dados;
- workflows de atualizacao.

## Resumo das Criacoes

Foram implementadas as seguintes entregas:

1. Home com pre-visualizacao de tres dashboards.
2. Componentes para `Especiais`, `Discricionarias e Legais` e `Fundo a Fundo`.
3. Pagina detalhada por dataset.
4. Filtros interativos com multiselecao.
5. Visualizacao de planilha com selecao de colunas.
6. Download de CSV filtrado.
7. Dark mode persistido em `localStorage`.
8. Camada de leitura e agregacao de dados locais no frontend.
9. Estilizacao responsiva para desktop e mobile.

## Estrutura Criada e Alterada

### Arquivos criados

```text
frontend/src/components/dashboard/DashboardPreview.tsx
frontend/src/pages/DatasetDetailPage.tsx
frontend/src/services/data/transferData.ts
public/data/especiais.csv
public/data/fundo-a-fundo.csv
public/data/discricionarias-legais.csv
```

### Arquivos alterados

```text
frontend/src/app/routes/router.tsx
frontend/src/components/layout/AppLayout.tsx
frontend/src/pages/HomePage.tsx
frontend/src/styles/global.css
```

## Fontes de Dados Utilizadas

O frontend foi implementado consumindo arquivos locais em `public/data/`.

```text
public/data/especiais.csv
public/data/fundo-a-fundo.csv
public/data/discricionarias-legais.csv
```

Origem desses arquivos:

| Arquivo publico | Origem no projeto | Processo de origem |
| --- | --- | --- |
| `public/data/especiais.csv` | `dataset/emendas_to.csv` | Gerado por `backend/api.py` |
| `public/data/fundo-a-fundo.csv` | `dataset/fundo_a_fundo.csv` | Gerado por `backend/api.py` |
| `public/data/discricionarias-legais.csv` | `data_discricionarias/processados/discricionarias_to.parquet` | Gerado por `backend/coletor_discricionarias.py` e convertido para CSV publico |

## Observacao Importante Sobre Backend

O frontend ainda nao consome uma API HTTP real.

A implementacao atual usa `fetch()` para ler CSVs estaticos dentro de `public/data/`. Isso permite validar layout, filtros, calculos, tabelas e experiencia de usuario sem depender de uma API backend.

Ainda falta implementar o backend para:

- expor endpoints de consulta;
- retornar dados de `Especiais`;
- retornar dados de `Fundo a Fundo`;
- retornar dados de `Discricionarias e Legais`;
- retornar KPIs e agregacoes, caso a estrategia seja processar no servidor;
- aplicar filtros no backend, se necessario;
- gerar downloads filtrados no servidor, se desejado;
- manter os dados sempre sincronizados com os ETLs.

## Roteamento

O arquivo `frontend/src/app/routes/router.tsx` foi atualizado para incluir a rota de detalhe:

```text
/                         -> HomePage
/dashboard/:datasetId     -> DatasetDetailPage
*                         -> NotFoundPage
```

Datasets atualmente aceitos:

```text
especiais
discricionarias-legais
fundo-a-fundo
```

## Home Page

A `HomePage` passou a renderizar:

- cabecalho contextual do dashboard;
- resumo consolidado de registros e valores;
- tres cards de pre-visualizacao;
- ranking resumido por dataset;
- botao para acessar cada pagina detalhada.

Os cards usam o componente:

```text
frontend/src/components/dashboard/DashboardPreview.tsx
```

## Componentes de Dashboard

### Especiais

Dataset configurado em `transferData.ts`:

```text
id: especiais
arquivo: /data/especiais.csv
coluna de valor: valor_total
ano: ano_emenda
status: situacao
agrupamento principal: parlamentar
entidade: beneficiario
```

Funcionalidades:

- KPIs de quantidade e valor total;
- ranking de beneficiarios;
- filtros por ano, situacao e parlamentar;
- planilha detalhada;
- download CSV filtrado.

### Discricionarias e Legais

Dataset configurado em `transferData.ts`:

```text
id: discricionarias-legais
arquivo: /data/discricionarias-legais.csv
coluna de valor: valor_repasse
ano: ano_assinatura
status: situacao
agrupamento principal: orgao_concedente
entidade: municipio_beneficiario
```

Funcionalidades:

- KPIs de convenios, valor repasse e valor pago;
- ranking por orgao concedente;
- evolucao por ano;
- filtros por anos, situacoes e orgaos;
- planilha detalhada;
- download CSV filtrado.

### Fundo a Fundo

Dataset configurado em `transferData.ts`:

```text
id: fundo-a-fundo
arquivo: /data/fundo-a-fundo.csv
coluna de valor: valor_total_repasse
ano: ano
status: situacao
agrupamento principal: sigla_orgao
entidade: municipio
```

Funcionalidades:

- KPIs de planos, total repasse e saldo disponivel;
- ranking por orgao;
- evolucao por ano;
- filtros por anos, situacoes e orgaos;
- planilha detalhada;
- download CSV filtrado.

## Camada de Dados

Foi criada a camada:

```text
frontend/src/services/data/transferData.ts
```

Responsabilidades:

- definir os datasets disponiveis;
- carregar CSVs estaticos via `fetch`;
- fazer parser de CSV separado por `;`;
- normalizar campos calculados, como `valor_total` e `ano`;
- calcular somatorios;
- calcular rankings;
- calcular series por ano;
- aplicar filtros;
- formatar numeros e moeda;
- gerar CSV para download.

### Tipos principais

```text
DatasetId
TransferRecord
DatasetConfig
DatasetSummary
AggregateRow
```

## Filtros

A pagina detalhada possui filtros globais para:

- anos;
- situacoes;
- agrupamento principal do dataset;
- busca textual na planilha.

Os filtros de anos, situacoes e agrupamentos usam multiselecao.

Regra aplicada:

```text
Sem selecao = todos os valores
Com selecao = apenas os valores selecionados
```

Exemplo de uso esperado:

```text
Selecionar 2020, 2021 e 2022 para comparar apenas esses anos.
```

## Multiselecao

O controle de multiselecao foi customizado para melhorar a experiencia do usuario.

Recursos implementados:

- botao compacto mostrando `Todos` ou quantidade selecionada;
- painel de opcoes;
- busca interna;
- chips removiveis;
- botao para selecionar opcoes visiveis;
- botao para limpar selecao;
- suporte a dark mode;
- comportamento responsivo.

Essa abordagem substituiu o uso visual do `select multiple` nativo, que era menos intuitivo e menos agradavel para uso em dashboard.

## Visualizacao de Planilha

A pagina detalhada inclui uma area de planilha com:

- tabela com registros filtrados;
- ordenacao por clique no cabecalho;
- limite visual inicial de 150 registros renderizados;
- selecao de colunas visiveis;
- resumo de filtros ativos;
- botao para limpar filtros;
- botao para baixar CSV filtrado.

O download usa:

```text
buildCsv()
```

em:

```text
frontend/src/services/data/transferData.ts
```

O arquivo baixado considera:

- filtros ativos;
- ordenacao aplicada;
- colunas atualmente visiveis.

## Dark Mode

Foi adicionado suporte a dark mode em:

```text
frontend/src/components/layout/AppLayout.tsx
frontend/src/styles/global.css
```

Caracteristicas:

- botao no topo da aplicacao;
- tema aplicado via `data-theme` no elemento raiz;
- preferencia persistida em `localStorage`;
- variaveis CSS para cores de fundo, superficie, borda, texto e destaque.

## Estilizacao

O arquivo `frontend/src/styles/global.css` foi expandido para cobrir:

- layout geral;
- topo da aplicacao;
- cards de dashboard;
- cards de KPI;
- graficos simples em CSS;
- filtros;
- multiselecao;
- chips;
- planilha;
- botoes;
- responsividade;
- dark mode.

### Ajuste de overflow em KPIs

Foi feito ajuste especifico nos cards de KPI para evitar que valores longos, como `Saldo disponivel`, ultrapassem o limite visual do card.

Alteracoes aplicadas:

- altura minima maior;
- quebra controlada de linha;
- `overflow-wrap: anywhere`;
- fonte responsiva;
- protecao contra overflow horizontal.

## Validacoes Realizadas

Validacao TypeScript:

```bash
yarn typecheck
```

Resultado:

```text
Concluido com sucesso.
```

Build frontend:

```bash
yarn build
```

Resultado:

```text
Concluido com sucesso.
```

Observacao: no ambiente Windows com sandbox, o Vite apresentou erro recorrente `spawn EPERM` ao executar `yarn build` dentro do sandbox. A validacao foi concluida com permissao elevada. Esse comportamento esta relacionado ao ambiente de execucao e ja havia ocorrido em validacoes anteriores.

Tambem foram verificados os arquivos publicos de dados em servidor local Vite:

```text
/data/especiais.csv
/data/fundo-a-fundo.csv
/data/discricionarias-legais.csv
```

Resultado:

```text
HTTP 200 para os tres arquivos.
```

## Impactos

### Impactos positivos

- O frontend agora possui uma experiencia inicial funcional.
- A estrutura React/Vite passou a ter rotas reais de dashboard.
- Os usuarios conseguem visualizar dados, filtrar, consultar tabela e baixar CSV.
- O dark mode melhora a flexibilidade visual.
- A camada de dados foi centralizada em um unico servico frontend.

### Impactos operacionais

- O dashboard React depende dos arquivos em `public/data/`.
- Se os ETLs gerarem novos dados, as copias publicas precisam ser atualizadas.
- Enquanto nao houver API backend, o navegador processa filtros e agregacoes localmente.

## Riscos de Regressao

| Risco | Probabilidade | Impacto | Mitigacao |
| --- | --- | --- | --- |
| Dados do frontend ficarem desatualizados | Alta | Medio | Automatizar atualizacao de `public/data/` ou criar API backend |
| Divergencia entre calculos React e Streamlit | Media | Alto | Comparar KPIs entre interfaces para filtros equivalentes |
| Performance cair com CSVs maiores | Media | Medio/Alto | Migrar filtros/agregacoes para backend ou paginar dados |
| Mudanca de esquema nos CSVs quebrar a tela | Media | Alto | Definir contrato de colunas e validar antes do render |
| Download CSV divergir do padrao esperado | Baixa/Media | Medio | Validar separador, encoding e colunas exportadas |
| Layout quebrar com valores monetarios muito longos | Baixa | Medio | Ajustes de KPI ja aplicados; manter QA visual em novas telas |

## Limitacoes Atuais

1. O frontend nao consome API HTTP real.
2. Os dados sao copias estaticas em `public/data/`.
3. O CSV de `Discricionarias e Legais` foi derivado do Parquet local.
4. A tabela renderiza visualmente ate 150 registros para evitar excesso de DOM.
5. Ainda nao ha paginacao real.
6. Ainda nao ha graficos com biblioteca dedicada.
7. Ainda nao ha testes automatizados de componentes.
8. A validacao visual por navegador integrado nao foi concluida porque a instancia `iab` nao estava disponivel na sessao.

## Pendencias Recomendadas para Backend

Para evoluir sem depender de dados estaticos, recomenda-se implementar endpoints como:

```text
GET /api/especiais
GET /api/especiais/resumo
GET /api/especiais/download

GET /api/fundo-a-fundo
GET /api/fundo-a-fundo/resumo
GET /api/fundo-a-fundo/download

GET /api/discricionarias-legais
GET /api/discricionarias-legais/resumo
GET /api/discricionarias-legais/download
```

Parametros sugeridos:

```text
anos
situacoes
grupo
busca
colunas
ordenarPor
direcao
pagina
limite
```

Com isso, o frontend podera deixar de processar grandes volumes no navegador e passara a consumir dados atualizados diretamente do backend.

## Recomendacoes Tecnicas

1. Criar API backend antes de remover os dados estaticos.
2. Definir contratos de resposta para cada dataset.
3. Preservar nomes de colunas ou criar mapeamento claro entre backend e frontend.
4. Validar equivalencia dos KPIs entre Streamlit, arquivos locais e API.
5. Implementar paginacao no backend para a planilha.
6. Criar endpoint especifico para download filtrado.
7. Automatizar a atualizacao dos dados publicos somente se a estrategia estatica for mantida temporariamente.
8. Adicionar testes para filtros, agregacoes e exportacao CSV.

## Conclusao

Foi criada uma primeira versao funcional do dashboard React para o DataCollector Seplan, cobrindo pre-visualizacao, detalhamento por dataset, filtros com multiselecao, planilha, download e dark mode.

A implementacao e adequada para validacao visual e funcional inicial do frontend, mas ainda depende de dados locais estaticos. A proxima etapa tecnica recomendada e a criacao do backend HTTP para fornecer dados, resumos, filtros e downloads diretamente ao frontend.
