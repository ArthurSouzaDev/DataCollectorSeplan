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

- **Situação do convênio (`situacao_convenio`):** vem do Portal da Transparência (CGU), por número de convênio.
  Ex.: *EM EXECUÇÃO*, *CONCLUÍDO*, *PRESTAÇÃO DE CONTAS APROVADA*. Propostas sem convênio aparecem como
  *Não se aplica (proposta)*. Convênios que não foram encontrados no Portal aparecem como
  *Sem correspondência no Portal*.
- **Valor liberado (`valor_desembolsado`):** valor liberado do convênio, pelo Portal da Transparência.

> **Limitação atual:** o Portal não traz valor pago, saldo em conta nem valores devolvidos. Esses campos aparecem
> como **Indisponível**, nunca como zero. Dos convênios de Tocantins, cerca de 78% foram encontrados no Portal.

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
| **Situações** | Mostra só as situações escolhidas (grupo da proposta) | `situacao` |
| **Situação do convênio** (só discricionárias) | Mostra só os convênios com a situação escolhida, como *EM EXECUÇÃO* | `situacao_convenio` |
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
- **Valor liberado:** soma do valor liberado dos convênios (Portal), mais ingresso de contrapartida e rendimento.
- **Contrapartida:** soma da contrapartida.
- **Saldo em conta:** **Indisponível.** Nenhuma fonte atual traz esse dado.
- **Valores devolvidos:** **Indisponível.** Mesmo motivo.
- **Valor pago:** **Indisponível.** Mesmo motivo.
- **Funil de execução:** mostra repasse → desembolsado, com percentual sobre o repasse. A etapa de pago e o
  saldo em conta aparecem como **Indisponível**, sem o funil fingir que o dado existe.

> Percentuais de "aprovadas" e de "assinatura de instrumentos" usam como base as propostas que já
> saíram da fase de cadastro. Propostas *Cadastradas* não entram no denominador.

---

## 4.1 Indicadores de convênios e ficha do registro

Esta seção vale para a base de **Discricionárias e legais**, exceto a ficha, que vale para todas as bases.

### Filtro e donut "Situação dos convênios"
- **Filtro "Situacao do convenio":** filtra pela situação do convênio no Portal (ex.: *EM EXECUÇÃO*).
  Não mexe no grupo da proposta.
- **Donut:** calculado só sobre os convênios (propostas não entram). Clicar numa fatia aplica o filtro.

### Vigência dos convênios
- **Cartões de 30, 90 e 180 dias:** quantidade de convênios cuja vigência termina nesse prazo e o repasse somado.
- Consideram só convênios em curso. Ficam de fora os concluídos, anulados, rescindidos, cancelados,
  arquivados e excluídos.
- **Vencendo primeiro** (recolhível): os 15 convênios com vigência mais próxima do fim, em até 180 dias.

### Execução abaixo de 50%
- **Execução abaixo de 50% com vigência próxima do fim** (recolhível): convênios com vigência em até 180 dias
  e valor liberado menor que 50% do repasse.
- Execução = valor liberado ÷ valor de repasse. Convênios sem repasse não entram.

### Ranking por município
- Os 10 municípios com maior valor de repasse. Mostra o total de todas as propostas filtradas.

### Situação dos convênios por ano
- Barras empilhadas por **ano de assinatura**, com a quantidade de convênios em cada situação.
- As 6 situações mais frequentes aparecem separadas; as demais ficam em "Outros".

### Ficha do registro
- **Como abrir:** clique em qualquer linha da planilha (ou selecione a linha e aperte Enter).
- **Onde aparece:** junto à linha clicada. Se a linha estiver na metade de baixo da tela, a ficha abre acima dela.
- **O que mostra:** número do registro, situação, cartões de valor (quando existirem) e os demais campos que a
  base já tem. Campo vazio aparece como "Não informado".
- **O que não mostra:** dados bancários de beneficiários e contatos de proponentes.
- **Como fechar:** botão ×, tecla Esc ou clique fora da ficha.

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

**Por que valor pago, saldo em conta e valores devolvidos aparecem como "Indisponível"?**
Nenhuma fonte atual traz esses dados. A API do Transferegov não os tem, e o Portal da Transparência também não.
Mostrar zero seria enganoso, então o painel mostra "Indisponível". O valor liberado (desembolso) vem do Portal e
aparece preenchido para os convênios encontrados.

**Existe outra fonte para esses dados?**
Não encontramos, nas APIs de convênios e despesas que consultamos, um vínculo que permita associar pagamentos
ou saldo bancário a cada convênio. Pagamentos existem por favorecido (CNPJ), mas não por convênio. Se essa fonte
for encontrada, o painel passa a mostrar os valores.

**Onde fica a situação "Em execução"?**
Ela é uma situação de convênio e está na coluna `situacao_convenio`, vinda do Portal. O filtro *Situações* e o
donut usam o grupo da proposta (o mesmo do painel da SERPRO), então "Em execução" não aparece lá. Veja a coluna
na tabela da planilha.

**Por que o total de convênios não bate com o painel oficial da SERPRO?**
A base usa o número de convênio informado nas metas de planos de trabalho. Convênios que não aparecem
nessa fonte não são contados. A comparação completa depende da integração com o Portal.

**Com que frequência os dados são atualizados?**
Fundo a fundo e especiais: a cada 12 horas. Discricionárias: diariamente. A data de "Atualizado em"
aparece no topo de cada painel.

**Como eu corrijo um número que parece errado?**
Abra a tela *Histórico* para ver o que mudou na última coleta. Se o problema vier da fonte, o ajuste é feito
no coletor, e o histórico registra a mudança.
