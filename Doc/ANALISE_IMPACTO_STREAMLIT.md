# Análise de Uso e Impacto de Substituição do Streamlit

Data da análise: 27/05/2026

Escopo analisado: branch local `main`, commit `2d00891`

Objetivo: identificar por que o sistema utiliza Streamlit, quais componentes dependem dele e o impacto de substituí-lo por outra solução de interface.

## Resumo executivo

O Streamlit é utilizado como camada de apresentação do dashboard, não como motor de coleta ou processamento de dados. Ele entrega a interface web, navegação por abas, filtros, indicadores, gráficos Plotly, cache de leitura e exportação dos resultados filtrados.

Há dependência direta do Streamlit em toda a experiência de consulta interativa:

- `app.py` é a aplicação principal e executa chamadas `st.*` desde a configuração da página até os downloads.
- `app_discricionarias.py` é renderizado dentro de `app.py` e também depende de `st.*`.
- `.devcontainer/devcontainer.json` inicia o projeto com `streamlit run app.py` e expõe a porta `8501`.
- `requirements.txt` instala o pacote `streamlit`.

Não foi identificada dependência do Streamlit na geração dos dados:

- `api.py` extrai e grava `emendas_to.csv`, `fundo_a_fundo.csv` e `cache_natureza.json`.
- `coletor_discricionarias.py` baixa, trata e grava `data_discricionarias/processados/discricionarias_to.parquet`.
- `.github/workflows/atualizar_dados.yml` executa os dois processos acima, sem executar `app.py` ou importar Streamlit.

Conclusão: remover Streamlit sem implementar outra interface interrompe o dashboard, mas não interrompe os ETLs nem a atualização automática dos arquivos de dados. Uma migração de frontend é viável, porém exige reimplementar toda a camada de visualização atualmente incorporada aos módulos Streamlit.

## Evidências da escolha do Streamlit

### Evidência explícita no repositório

- O histórico contém o commit `960df3e` de 07/04/2026, com a mensagem `Adicionando Dashboard com streamlit`.
- O `README.md` declara a separação entre ETL e interface Streamlit e orienta iniciar o dashboard com `streamlit run app.py` (`README.md:57`, `README.md:132`, `README.md:150`).
- O devcontainer abre `app.py`, instala Streamlit e sobe automaticamente o servidor (`.devcontainer/devcontainer.json:9`, `:20`, `:22`).

### Motivo técnico inferido da implementação

Não existe no repositório uma ADR ou justificativa formal da decisão tecnológica. Pela implementação, o Streamlit foi adotado para construir rapidamente um dashboard analítico Python sobre arquivos locais, reutilizando diretamente `pandas` e `plotly`, sem criar API de consulta nem frontend separado.

As capacidades efetivamente aproveitadas são:

- renderização de layout, abas, cartões e tabelas;
- controles reativos para filtros;
- apresentação de gráficos Plotly;
- download de CSV filtrado no navegador;
- cache em memória das leituras de CSV e Parquet;
- execução local simples por um único comando.

## Arquitetura atual

```text
Fontes externas
  |-- APIs configuradas para transferências especiais/fundo a fundo
  |-- ZIPs públicos SICONV/Transferegov para discricionárias
  v
Processamento independente da interface
  |-- api.py ----------------------------> emendas_to.csv
  |                                      -> fundo_a_fundo.csv
  |                                      -> cache_natureza.json
  |-- coletor_discricionarias.py --------> data_discricionarias/processados/discricionarias_to.parquet
  v
Camada de apresentação Streamlit
  |-- app.py lê os dois CSVs
  |-- app.py importa app_discricionarias.py
  |-- app_discricionarias.py lê o Parquet
  v
Dashboard com abas, filtros, KPIs, gráficos, tabela e downloads
```

## Mapa de dependências

| Componente | Dependência de Streamlit | Evidência | Consequência da remoção imediata |
| --- | --- | --- | --- |
| `app.py` | Direta e integral | `import streamlit as st` (`app.py:2`) e chamadas `st.*` ao longo do módulo | Não inicia; perde todo o dashboard principal |
| `app_discricionarias.py` | Direta e integral | `import streamlit as st` (`app_discricionarias.py:4`) e `render()` (`:125`) | A aba discricionárias não pode ser renderizada |
| Integração entre abas | Indireta | `app.py` importa e chama `app_discricionarias.render()` (`app.py:7`, `:302`) | Mesmo trocar apenas `app.py` exige migrar ou adaptar a aba importada |
| Cache de leitura | Direta | `@st.cache_data` (`app.py:47`, `:62`; `app_discricionarias.py:74`) | Novas leituras precisam de cache equivalente ou terão impacto de desempenho |
| Filtros e estado da tela | Direta | `st.tabs`, `selectbox`, `multiselect`, chaves de widgets | Seleções e atualização reativa deixam de existir |
| Gráficos e indicadores | Direta na renderização | `st.plotly_chart`, `st.metric` | Cálculos podem ser reaproveitados, mas exibição deve ser reimplementada |
| Downloads filtrados | Direta | `st.download_button` (`app.py:289`, `:508`; `app_discricionarias.py:503`) | Usuário perde exportação interativa |
| Devcontainer | Direta operacional | Comando `streamlit run app.py` e porta `8501` | Ambiente de desenvolvimento deixa de subir a aplicação |
| Dependências Python | Direta operacional | `streamlit` em `requirements.txt:1` | Remoção antes da migração causa `ModuleNotFoundError`/comando indisponível |
| `api.py` | Nenhuma identificada | Não importa Streamlit; grava CSVs (`api.py:281-290`) | Continua funcional, preservadas as demais dependências/configurações |
| `coletor_discricionarias.py` | Nenhuma identificada | Não importa Streamlit; grava Parquet (`coletor_discricionarias.py:383`) | Continua funcional |
| Workflow de atualização | Nenhuma em runtime | Executa `python api.py` e `python coletor_discricionarias.py` (`.github/workflows/atualizar_dados.yml:32-39`) | Continua gerando dados; a instalação de Streamlit passa a ser desnecessária após migração |

## Funcionalidades hoje acopladas à interface

### Aba Especiais

Implementada em `app.py`, lê `emendas_to.csv` e fornece:

- filtros por ano, situação, parlamentar, município e natureza jurídica;
- KPIs de quantidade, valor total, custeio e investimento;
- gráficos de ranking, distribuição e evolução;
- tabela detalhada e download do CSV filtrado.

### Aba Fundo a Fundo

Implementada em `app.py`, lê `fundo_a_fundo.csv` e fornece:

- filtros por ano, situação, órgão e natureza jurídica;
- KPIs financeiros;
- gráficos de ranking, distribuição, evolução e composição;
- tabela detalhada e download do CSV filtrado.

### Aba Discricionárias e Legais

Implementada em `app_discricionarias.py`, lê o Parquet processado e fornece:

- harmonização de nomes de colunas e validação de esquema;
- filtros por anos, situação, órgão, município, natureza jurídica e proponente;
- KPIs e gráficos de execução financeira;
- tabela detalhada e download do CSV filtrado.

Observação importante: além da renderização, parte da preparação dos dados para consulta está dentro dos arquivos de UI. Uma migração limpa deve separar carregamento, normalização, filtros e agregações da nova tecnologia de apresentação.

## Impacto de uma mudança

### Cenário A: remover Streamlit sem substituição

Impacto: crítico para usuários do dashboard e baixo para o pipeline de dados.

Quebra imediatamente:

- inicialização por `streamlit run app.py`;
- todas as três abas do dashboard;
- filtros interativos, KPIs, tabelas e gráficos;
- downloads de relatórios filtrados;
- inicialização automática no devcontainer.

Permanece operacional:

- coleta das APIs em `api.py`, desde que suas configurações de execução estejam disponíveis;
- processamento Parquet em `coletor_discricionarias.py`;
- workflow agendado que atualiza os arquivos de dados.

### Cenário B: substituir por outro frontend mantendo arquivos como fonte

Impacto: médio/alto, com menor alteração no pipeline.

O novo frontend pode continuar consumindo:

- `emendas_to.csv`;
- `fundo_a_fundo.csv`;
- `data_discricionarias/processados/discricionarias_to.parquet`.

Será necessário reimplementar:

- carregamento e cache;
- filtros e estado de navegação;
- cálculos de KPIs e agregações;
- gráficos e tabelas;
- downloads filtrados;
- mensagens de erro/validação de esquema;
- configuração de execução e publicação.

Risco principal: duplicar regras hoje embutidas na tela e gerar valores divergentes entre a versão atual e a nova.

### Cenário C: criar API backend e frontend separado

Impacto: alto, porém reduz acoplamento futuro.

Além dos itens do cenário B, será necessário:

- definir endpoints e contratos para filtros, agregações e downloads;
- decidir estratégia de leitura dos arquivos ou persistência em banco;
- configurar CORS, autenticação/autorização caso aplicável e implantação de dois componentes;
- versionar e testar contratos da API.

Esse cenário é justificável se houver necessidade de múltiplos consumidores, controle de acesso, escalabilidade ou evolução independente do frontend. Não é obrigatório apenas para retirar Streamlit.

## Arquivos afetados em uma substituição

| Arquivo/artefato | Alteração esperada | Risco |
| --- | --- | --- |
| `app.py` | Substituir ou deixar de usar como ponto de entrada Streamlit; extrair regras reaproveitáveis | Alto: contém duas abas completas e integração da terceira |
| `app_discricionarias.py` | Migrar renderização e extrair normalização/cálculos | Alto: valida esquema e calcula indicadores financeiros |
| `requirements.txt` | Remover `streamlit` apenas após nova interface estar funcional; incluir somente dependências necessárias | Médio: remoção antecipada indisponibiliza a UI atual |
| `.devcontainer/devcontainer.json` | Alterar comando de inicialização, porta e preview | Médio: ambiente local pode não subir |
| `README.md` | Atualizar instruções de execução e arquitetura | Baixo funcional, alto operacional |
| `.github/workflows/atualizar_dados.yml` | Opcionalmente evitar instalar dependência de UI no job ETL ou separar requisitos | Baixo se a coleta continuar inalterada |
| Testes a criar | Cobrir regras extraídas, contratos e equivalência visual/numérica | Alto se omitido |

Arquivos que não precisam mudar para uma troca apenas da interface, desde que os contratos de dados sejam preservados:

- `api.py`;
- `coletor_discricionarias.py`;
- CSVs e Parquet gerados pelo pipeline.

## Mudanças recomendadas antes da substituição

1. Extrair de `app.py` e `app_discricionarias.py` funções independentes de UI para leitura, harmonização, aplicação de filtros e cálculo de KPIs.
2. Definir contratos explícitos para os três datasets: arquivos consumidos, colunas obrigatórias, tipos e valores padrão.
3. Criar testes de regressão para totais, filtros e exportações usando os artefatos atuais como amostra.
4. Implementar a nova interface consumindo as funções extraídas ou uma API, mantendo a interface Streamlit disponível durante validação.
5. Comparar resultados das duas interfaces para os mesmos filtros antes do corte.
6. Somente após aceite funcional, remover a inicialização Streamlit, a dependência e a documentação antiga.

## Riscos de regressão

| Risco | Probabilidade | Impacto | Mitigação |
| --- | --- | --- | --- |
| KPIs diferentes devido a regra reimplementada incorretamente | Alta | Alto | Extrair regras existentes e criar testes comparativos |
| Filtros não reproduzirem combinações e valores padrão atuais | Média | Alto | Testes por aba e cenários com filtros combinados |
| Perda de exportação CSV ou alteração de encoding/separador | Média | Médio | Validar arquivos baixados em UTF-8-SIG com separador `;` |
| Performance inferior ao perder `st.cache_data` | Média | Médio | Medir tempo/memória e adotar cache equivalente |
| Incompatibilidade com mudanças no esquema do Parquet | Média | Alto | Manter `harmonizar_colunas()`/validação ou extrair regra equivalente |
| Ambiente de desenvolvimento sem comando/porta corretos | Alta se ignorado | Médio | Atualizar devcontainer e instruções junto à migração |
| Interrupção precoce do dashboard atual | Média | Alto | Fazer migração paralela e retirar Streamlit somente após homologação |

## Recomendação técnica

O sistema depende de Streamlit apenas para disponibilizar a interface analítica, mas essa interface é atualmente o produto consumível pelo usuário. Portanto, a retirada direta não é uma limpeza de dependência: é a remoção integral do dashboard.

Caso a motivação seja manutenção, identidade visual ou publicação em ambiente mais controlado, a abordagem de menor risco é:

1. desacoplar primeiro as regras de transformação e cálculo da UI;
2. preservar os arquivos de saída atuais como contrato inicial;
3. implementar a nova interface em paralelo;
4. validar paridade de filtros, indicadores e downloads;
5. remover Streamlit somente após a nova interface assumir a operação.

## Limitações da análise

- A análise foi realizada sobre os arquivos versionados e o histórico local disponível; não foi validada uma implantação externa que possa possuir configuração adicional.
- O arquivo `.env` não foi acessado, em conformidade com a orientação de segurança. Isso não impede avaliar o acoplamento ao Streamlit, pois as variáveis são utilizadas por `api.py`, e não pela interface analisada.
- A branch local está atrás de `origin/main` em commits de automação/dados já presentes na referência local; não foi identificada diferença de código da aplicação nos arquivos analisados.
