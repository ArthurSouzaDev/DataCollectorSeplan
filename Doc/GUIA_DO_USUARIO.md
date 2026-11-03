# Guia do usuário — DataCollector BI (SEPLAN/TO)

Painel de transferências federais para o Tocantins: fundo a fundo, transferências especiais
e discricionárias/legais. Este guia explica de onde vêm os dados, o que significa cada número
e como usar os filtros.

---

## 1. Fontes de dados

### 1.1 Fundo a fundo

- **O que é:** planos de ação de transferências fundo a fundo (saúde, assistência, educação etc.)
  cujo ente recebedor é do Tocantins.
- **Origem:** API pública do Transferegov (módulo fundo a fundo).
- **Arquivo publicado:** `fundo-a-fundo.csv`.
- **Atualização:** a cada 12 horas (workflow *Atualizar Fundo a Fundo e Especiais*).
- **Colunas principais:** `codigo_plano`, `situacao`, `municipio`, `sigla_orgao`, `fundo_repassador`,
  `valor_emenda`, `valor_especifico`, `valor_voluntario`, `valor_total_repasse`, `valor_total_plano`,
  `saldo_disponivel`, `natureza_juridica`.
- **Como o ano é calculado:** a partir da data de início da vigência do plano.

### 1.2 Transferências especiais

- **O que é:** emendas parlamentares especiais (planos de ação especiais) destinadas a beneficiários do Tocantins.
- **Origem:** API pública do Transferegov (planos de ação especiais). O coletor primeiro lista os
  beneficiários do TO e depois busca os planos de cada um.
- **Arquivo publicado:** `especiais.csv`.
- **Atualização:** a cada 12 horas.
- **Colunas principais:** `codigo_plano`, `ano_emenda`, `parlamentar`, `beneficiario`, `situacao`,
  `valor_custeio`, `valor_investimento`, `natureza_juridica`.
- **Valor total:** custeio + investimento.

### 1.3 Discricionárias e legais

- **O que é:** propostas de transferências voluntárias (discricionárias) e legais do Tocantins,
  com o número do convênio quando a proposta foi formalizada.
- **Origem:** API pública do Transferegov (propostas e planos de trabalho/metas de TO).
- **Arquivo publicado:** `discricionarias-legais.csv`.
- **Atualização:** diária, às 03:17 (horário de Brasília).
- **Situação:** é o grupo da proposta — *Aprovadas*, *Rejeitadas*, *Cadastradas*, *Em Análise pelo Órgão
  Concedente* ou *Em Complementação pelo Estado/Município*. É o mesmo agrupamento do painel oficial da SERPRO.
- **Fase:** `Convênio` quando a proposta tem número de convênio; `Proposta` nos demais casos.
- **Valores:** `valor_global` e `valor_repasse` são os valores **da proposta**, somados para todas as
  propostas de TO.

> **Limitação atual:** a API do Transferegov não traz desembolso, valor pago, saldo em conta nem valores
> devolvidos. Essas colunas aparecem zeradas. A integração com o Portal da Transparência (CGU), que
> traz valor liberado e situação de convênio, está em preparação e depende de chave de acesso.

### 1.4 Histórico de atualizações

Cada execução dos workflows compara a base nova com a anterior e registra o que mudou (registros novos,
removidos, campos alterados, totais e situações). O histórico guarda os **últimos 90 dias** e está
na tela *Histórico*.

---

## 2. Como navegar

- **Início:** lista as três bases, com o total de registros e a data da última atualização.
- **Módulos (menu superior):** *Fundo a Fundo*, *Especiais*, *Discricionárias e Legais*. Cada um abre um painel.
- **Histórico:** o que mudou em cada coleta, separado por base (aba *Todas* ou uma base específica).
  Clique numa execução para ver os detalhes.
- **Ajuda:** resumo deste guia.
- **Tema claro/escuro:** botão de lua/sol no canto superior direito.
- **Celular:** o menu fica no ícone de três barras.

---

## 3. Como usar os filtros

Os filtros ficam no topo de cada painel. Todos os KPIs, gráficos e a tabela reagem juntos.

| Filtro | O que faz | Coluna usada |
|---|---|---|
| **Anos** | Mostra só os anos selecionados | ano da base (ver nota abaixo) |
| **Situações** | Mostra só as situações escolhidas | `situacao` |
| **Agrupamento** (rótulo varia por base) | Filtra por órgão/parlamentar/órgão repassador | ver tabela abaixo |
| **Natureza jurídica** | Filtra pela natureza do proponente/beneficiário | `natureza_juridica` |
| **Somente convênios formalizados** | Mantém só registros com convênio ativo (não cancelado) | `fase` (discricionárias); situação nas demais |
| **Buscar na planilha** | Texto livre sobre as colunas visíveis da tabela | colunas da tabela |

**Agrupamento por base:**

| Base | Agrupamento |
|---|---|
| Especiais | parlamentar |
| Discricionárias e legais | órgão concedente |
| Fundo a fundo | sigla do órgão |

**Como usar cada controle:**

- **Selecionar em listas:** clique no campo, marque as opções desejadas ou use *Selecionar visíveis*.
  Os itens escolhidos aparecem como chips; clique no **x** de um chip para remover.
- **Sem seleção = todos.** Se nenhum item estiver marcado, o filtro não é aplicado.
- **Clicar em gráficos:** clicar numa barra do ranking ou numa fatia do donut aplica o filtro daquele item.
  O botão *Limpar seleção* desfaz. Clicar de novo no mesmo item também remove.
- **Limpar filtros:** botão no cabeçalho da tabela. Zera todos os filtros e a busca.
- **Colunas visíveis:** escolha quais colunas aparecem na tabela. Sem escolha, aparecem todas as relevantes.
- **Ordenar a tabela:** clique no nome da coluna. Um clique ordena em ordem decrescente; o segundo inverte.
- **Baixar CSV:** exporta **só** os registros filtrados, com as colunas visíveis, separador `;` e codificação UTF-8.

**Atenção:**
- Na base de discricionárias, o filtro de **Anos** usa o ano de assinatura do convênio. Propostas sem convênio
  não têm esse ano, então somem quando um ano é selecionado.
- A tabela mostra no máximo **150 linhas**. O contador ("registros filtrados") mostra o total real;
  para ver tudo, use *Baixar CSV*.

---

## 4. Como ler os indicadores

### Fundo a fundo
- **Planos:** quantidade de planos filtrados.
- **Total repasse:** soma de `valor_total_repasse`.
- **Via emenda:** parcela do repasse vinda de emenda parlamentar (percentual sobre o total).
- **Específico** e **Voluntário:** demais partes do repasse.
- **Valor total dos planos:** soma de `valor_total_plano`.

### Especiais
- **Total de especiais:** quantidade de planos.
- **Valor total:** custeio + investimento.
- **Custeio** e **Investimento:** cada parcela, com o percentual sobre o total.

### Discricionárias e legais
- **Registros:** total de propostas filtradas. Quando há fase, o subtítulo mostra quantos são convênios e quantos são propostas.
- **Valor global:** repasse + contrapartida.
- **Valor liberado:** soma de desembolsado, contrapartida ingressada e rendimento. **Hoje zerado** (ver limitação).
- **Contrapartida:** soma da contrapartida.
- **Saldo em conta:** saldo positivo em conta. **Hoje zerado.**
- **Valores devolvidos:** saldo devolvido ao tesouro e pelo convenente. **Hoje zerado.**
- **Valor pago:** soma de valor pago. **Hoje zerado.**
- **Funil de execução:** repasse → desembolsado → pago, com percentual sobre o repasse. **Hoje sem dados** nessa base.

> Percentuais de "aprovadas" e de "assinatura de instrumentos" usam como base as propostas que já
> saíram da fase de cadastro. Propostas *Cadastradas* não entram no denominador.

---

## 5. Histórico de atualizações

A tela *Histórico* mostra cada execução com:
- **Registros:** antes → depois, com a variação.
- **Novos e removidos:** quantidade e exemplos de identificadores.
- **Campos alterados:** quantos registros tiveram cada campo modificado.
- **Totais e situações:** variação dos valores e das contagens por situação.

Use a aba de cada base para filtrar. A primeira execução de cada base aparece como *Primeira execução*,
porque não há versão anterior para comparar.

---

## 6. Perguntas frequentes

**Por que os valores de desembolso e pagamento estão zerados?**
A API pública usada hoje não fornece essas informações. A integração com o Portal da Transparência
está em preparação.

**Por que não encontro a situação "Em execução"?**
Ela é uma situação de convênio. A base de discricionárias agora mostra o grupo da proposta, que não inclui
essa situação. Ela volta quando o Portal da Transparência for integrado.

**Por que o total de convênios não bate com o painel oficial da SERPRO?**
A base usa o número de convênio informado nas metas de planos de trabalho. Convênios que não aparecem
nessa fonte não são contados. A comparação completa depende da integração com o Portal.

**Com que frequência os dados são atualizados?**
Fundo a fundo e especiais: a cada 12 horas. Discricionárias: diariamente. A data de "Atualizado em"
aparece no topo de cada painel.

**Como eu corrijo um número que parece errado?**
Abra a tela *Histórico* para ver o que mudou na última coleta. Se o problema vier da fonte, o ajuste é feito
no coletor, e o histórico registra a mudança.
