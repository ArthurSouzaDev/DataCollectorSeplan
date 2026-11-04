# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).
Commits seguem [Conventional Commits](https://www.conventionalcommits.org/pt-br/v1.0.0/).

## [Não publicado]

### Adicionado
- Integração dos convênios do Portal da Transparência (CGU) às discricionárias: situação de execução
  (`situacao_convenio`) e valor liberado (`valor_desembolsado`), pela junção com o número SICONV.
- Histórico de atualizações (`/historico`), com comparação entre execuções e retenção de 90 dias.
- Tela de Ajuda (`/ajuda`), acessada pelo ícone "?" ao lado do tema, com resumo do guia do usuário.
- Guia do usuário em `Doc/GUIA_DO_USUARIO.md`: fontes, filtros, indicadores e perguntas frequentes.
- Discricionárias: filtro e donut de situação do convênio, calculados só sobre os convênios.
- Discricionárias: cartões de vigência (30, 90 e 180 dias), lista de vencimentos próximos e lista de
  convênios com execução abaixo de 50%, ambas recolhíveis.
- Discricionárias: ranking por município e gráfico de situação dos convênios por ano de assinatura.
- Ficha do registro, aberta ao clicar em qualquer linha da planilha de todas as bases.
- Especiais: ranking por beneficiário, situação por ano da emenda e participação por parlamentar.
- Fundo a fundo: ranking por município, situação por ano e participação por fundo repassador.
- Filtro por clique nos gráficos complementares, com resumo dos filtros ativos.
- Legendas clicáveis nos rankings, nas séries por ano e nas roscas. Com mais de 4 itens, recolhem.

### Alterado
- Situação das propostas passa a usar `sit_proposta_grupo`, o mesmo agrupamento do painel da SERPRO.
- Workflows separados: fundo a fundo e especiais a cada 12 horas; discricionárias diariamente.
- Valor pago, saldo em conta e valores devolvidos aparecem como "Indisponível", em vez de zero.

### Removido
- Workflow `atualizar_dados.yml`, substituído pelos dois workflows separados.

## Convenções
- Tipos de commit: `feat`, `fix`, `docs`, `ci`, `refactor`, `chore`.
- Commits automáticos do bot usam o prefixo `🤖 [AUTO]` e não entram neste changelog.
