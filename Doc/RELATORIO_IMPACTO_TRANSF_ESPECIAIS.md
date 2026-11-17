# Relatório de impacto: consumo completo da API de Transferências Especiais

Branch: `Feature-Consumo-transf-especial`
Documentação da API: https://api-publica.transferegov.gestao.gov.br/especiais/docs

---

## 1. O que foi alterado

Foi criado um módulo novo em `backend/especiais/`, que consome os **24 endpoints** da API de
Transferências Especiais e grava um CSV por endpoint.

| Arquivo | Função |
|---|---|
| `backend/especiais/endpoints.py` | Registro declarativo dos 24 endpoints, com as dependências entre eles |
| `backend/especiais/cliente.py` | Cliente HTTP: retry, paginação (200 por página) e erros |
| `backend/especiais/coleta.py` | Orquestra a coleta, grava os CSVs e o `_manifest.json` |
| `backend/especiais/__init__.py` | Marca o pacote |

Saída: `dataset/transf_especiais/<dataset>.csv` e `dataset/transf_especiais/_manifest.json`.

Execução:

```bash
python -m backend.especiais.coleta                          # coleta completa (TO)
python -m backend.especiais.coleta --limite-beneficiarios 3 # teste rápido
python -m backend.especiais.coleta --workers 6              # mais consultas paralelas
```

### Como a coleta funciona

A árvore parte dos beneficiários do Tocantins (`uf_beneficiario=TO`). Cada dataset dependente é
consultado uma vez por valor distinto da coluna de origem:

```
beneficiarios (TO)
 └─ planos_acao  (id_beneficiario)
     ├─ planos_acao_historico, executores, devolucao, relatorios_gestao, relatorios_gestao_novos,
     │   empenhos, planos_trabalho  (id_plano_acao)
     ├─ gestao_financeira_lancamentos, saldo_conta  (id_agencia_conta)
     ├─ meta, finalidade  (id_executor)
     ├─ documentos_habeis (id_empenho) → ordens_pagamento (id_dh)
     ├─ planos_trabalho_historico, planos_trabalho_analises, orgaos_analises_pendentes (id_plano_trabalho)
     └─ ... (relatórios de gestão, análises, subtransações)
```

Os endpoints sem filtro por UF (`planos_acao_historico`, `gestao_financeira_lancamentos` etc.) só são
consultados a partir dos IDs do Tocantins. Sem isso, a coleta traria a base nacional inteira
(ex.: 746 mil lançamentos financeiros, 159 mil metas).

---

## 2. Impacto no sistema

### Não muda
- **Painel React** (`frontend/`): nenhuma alteração. Ele continua lendo `public/data/especiais.csv`.
- **`backend/api.py`**: não foi tocado. Ele continua gerando `dataset/emendas_to.csv` e
  `dataset/fundo_a_fundo.csv`, que alimentam o painel.
- **`public/data/` e `manifest.json`**: não mudam. Nada é sincronizado para o site.
- **Dependências**: usa só `requests` e `pandas`, que já estão em `requirements.txt`.

### Muda
- Passa a existir uma pasta nova, `dataset/transf_especiais/`, com 24 CSVs e um manifesto.
- Há mais chamadas à API pública, feitas pelo módulo novo (ver seção 3).

### Medido no teste (3 beneficiários, ~60 s)

| Dataset | Registros | Observação |
|---|---|---|
| beneficiarios | 3 | recorte limitado para o teste |
| planos_acao | 38 | |
| planos_acao_historico | 96 | |
| planos_trabalho | 38 | |
| planos_trabalho_historico | 272 | |
| gestao_financeira_lancamentos | 521 | |
| saldo_conta | 22 | |
| meta / finalidade | 62 / 44 | |
| empenhos / documentos_habeis / ordens_pagamento | 39 / 36 / 32 | |
| devolucao, planos_trabalho_analise_historico, gestao_financeira_subtransacoes | **0** | ver seção 5 |

Erros de API no teste: **0**. Tempo total: **59,9 s** para 3 beneficiários, com `workers=4`.

### Medido na coleta completa (TO, 140 beneficiários, `workers=6`)

- Tempo total: **1.467 s (~24,5 min)**, código de saída 0.
- 24 datasets, **126.941 registros**, **0 erros** de API.
- Maior tempo: `planos_trabalho_analise_historico`, 44.666 registros em 396 s. Os endpoints dependentes levam ~55 a 80 s cada.
- Os 24 datasets estão em `dataset/transf_especiais/`, com `_manifest.json`.

---

## 3. Impacto no workflow (GitHub Actions)

Hoje o workflow `.github/workflows/atualizar_fundo_especiais.yml` roda `backend/api.py` a cada 12 h,
com timeout de **90 min**. **O módulo novo ainda não está no workflow**, então nada muda na rotina
automática até que seja incluído.

Se for incluído no workflow atual, o impacto seria:

1. **Tempo de execução.** Estimativa, ainda não medida para a base completa: o TO tem ~140
   beneficiários (`beneficiarios-especiais?uf=TO` retornou 140 registros). Com ~38 planos para 3
   beneficiários, a estimativa é de 1.959 planos, e portanto cerca de 15 mil chamadas HTTP. Medido na coleta completa: **~33 min com 4 workers** (padrão do coletor) e **~24,5 min com 6 workers**.
   Ambos cabem no limite de 90 min do job, com 0 erros nos dois casos. A margem de ~57 min com 4 workers
   depende da carga da API no momento da execução.
2. **Carga na API pública.** Não há limite de taxa documentado. O módulo usa 4 consultas em
   paralelo por padrão e reexecuta falhas 429/5xx com espera crescente (2 s, 4 s). Um aumento de
   `--workers` acelera a coleta, mas aumenta a carga sobre o serviço do governo.
3. **Commit.** O passo `Commitar dados atualizados` faz `git add` apenas de
   `dataset/fundo_a_fundo.csv`, `dataset/emendas_to.csv`, `dataset/cache_natureza.json` e
   `public/data/`. Os CSVs novos **não entram no commit** enquanto a pasta não for adicionada. Se
   forem, cada execução a cada 12 h gera um diff grande no histórico do Git, o que é um custo a
   considerar.
4. **Código de saída.** O coletor retorna código 1 se alguma consulta falhar. Dentro do workflow
   isso derrubaria o job inteiro, inclusive o passo que atualiza o fundo a fundo. Recomenda-se
   rodar o módulo novo em um passo separado, com `continue-on-error: true`, ou em um workflow próprio.
5. **Dependência de segredos.** O módulo usa a URL padrão da API e não precisa de secret. A variável
   `API_ESPECIAIS_BASE_URL` é opcional.

**Recomendação:** não alterar o workflow existente ainda. Primeiro rodar a coleta completa
localmente, medir o tempo e validar os dados. Depois decidir entre (a) um workflow separado, com
frequência diária, ou (b) um passo adicional no workflow atual.

---

## 4. Riscos e limitações conhecidas

- **Filtros não validados um a um.** A API aceitou todos os parâmetros de encadeamento (nenhum HTTP
  422), mas a validação de que cada chave traz os registros esperados só foi feita para parte dos
  endpoints (ver seção 5).
- **Falhas parciais.** Se uma consulta dependente falhar após as tentativas, o dataset sai incompleto
  e a falha fica registrada em `_manifest.json` (`erros`, `exemplos_erro`). O coletor não para por
  causa de uma falha isolada.
- **Chaves que não existem na resposta.** Quando a resposta não traz o campo usado como parâmetro,
  o coletor acrescenta a coluna com o valor consultado (`setdefault`), para manter a ligação entre
  os CSVs.
- **Volume em disco.** Como o recorte é do TO, o volume deve ser pequeno, mas não foi medido. A
  coleta completa gera 24 arquivos.
- **Sem testes automatizados.** Só houve teste manual com amostra. Não há testes unitários para
  `cliente.py` nem para `coleta.py`.

---

## 5. Pendências de validação

Três datasets retornaram **0 registros** no teste, o que pode indicar nome de campo errado ou
simplesmente ausência de dados para essa amostra:

1. `devolucao`: 0 registros. Pode ser que o TO não tenha devoluções nessa amostra, ou que o filtro
   `id_plano_acao` precise de outro valor. **Validar com um plano que tenha devolução.**
2. `planos_trabalho_analise_historico`: 0 registros, usando `id_plano_trabalho_analise_pt`.
   **Validar o nome do parâmetro na documentação.**
3. `gestao_financeira_subtransacoes`: 0 registros em 521 consultas, usando
   `id_lancamento_gestao_financeira`. Este é o caso mais suspeito, porque a chave vem de um dataset
   com 521 lançamentos. **Validar com um lançamento específico, consultado diretamente na API.**

Também é preciso confirmar o tempo da coleta completa, com todos os beneficiários do TO, e medir
quantas chamadas são feitas.

---

## 6. Próximos passos sugeridos

1. Validar as três pendências da seção 5.
2. Rodar a coleta completa localmente e medir tempo, número de chamadas e tamanho dos CSVs.
3. Decidir como a coleta entra no workflow (seção 3).
4. Quando o painel for usar os dados novos, criar a sincronização para `public/data/` e a entrada
   correspondente no `manifest.json`.
5. Acrescentar entrada em `CHANGELOG.md`, em "Adicionado", quando a funcionalidade estiver validada.

---

## 7. Atualização: workflow incluído e painel

### Workflow (`atualizar_fundo_especiais.yml`)
- Novo passo `python -m backend.especiais.coleta --workers 4`, logo após `backend/api.py`.
- O passo tem `continue-on-error: true`: uma falha na coleta não impede a atualização do fundo a fundo e das emendas.
- `dataset/transf_especiais/` entrou no `git add` do commit automático.
- Tempo medido com 4 workers: ~33 min, dentro do limite de 90 min do job.

### Coleta com staging
- A coleta grava numa pasta `_novo` e só substitui `dataset/transf_especiais/` se terminar sem erro.
- Com falha, a saída anterior fica intacta, e o conjunto com erro vai para `_falha/` para inspeção.
- Assim, um conjunto parcial nunca é publicado.

### Painel `/transf-especiais`
- 6 grupos, 22 gráficos, lidos de `public/data/transf_especiais/`.
- Indicadores: planos, valor dos planos, empenhado, documentos hábeis, lançamentos e saldo atual.
- O sync (`scripts/sync_public_data.py`) copia a pasta inteira para `public/data/`.
- Verificado no navegador (dev server): página sem erros de console; painel `/dashboard/especiais` sem regressão.

### Observações de dados
- Todos os 1.959 planos têm ao menos um empenho (2.048 empenhos no total), e a soma dos empenhos é igual à soma dos planos (R$ 1.028.321.058,52). Por isso "Valor dos planos" e "Empenhado" aparecem iguais no painel.
- `relatorios_gestao_analise` veio vazio na coleta completa. O painel não tem gráfico para esse dataset.
- Gráficos de rosca são por quantidade de registros, não por valor.
