import { parseCsv, toNumber, type TransferRecord } from './transferData';

/**
 * Módulo completo de Transferências Especiais.
 *
 * Os CSVs são gerados por `python -m backend.especiais.coleta` e copiados para
 * `public/data/transf_especiais/` pelo `scripts/sync_public_data.py`.
 *
 * Este arquivo define três coisas: quais datasets carregar, como derivar colunas
 * que os gráficos precisam (ano, valor total, nome do beneficiário…) e quais
 * gráficos aparecem em cada grupo.
 */

export const BASE_TRANSF_ESPECIAIS = '/data/transf_especiais';

/** Coluna sintética com 1 em cada linha: transforma "soma" em "contagem" nos gráficos. */
export const COLUNA_REGISTRO = '__registro';

export const NOMES_DATASETS = [
  'data_atualizacao',
  'programas',
  'beneficiarios',
  'planos_acao',
  'planos_acao_historico',
  'executores',
  'meta',
  'finalidade',
  'devolucao',
  'planos_trabalho',
  'planos_trabalho_historico',
  'planos_trabalho_analises',
  'planos_trabalho_analise_historico',
  'orgaos_analises_pendentes',
  'relatorios_gestao',
  'relatorios_gestao_novos',
  'relatorios_gestao_analise',
  'relatorios_gestao_documento_liquidacao',
  'empenhos',
  'documentos_habeis',
  'ordens_pagamento',
  'gestao_financeira_lancamentos',
  'gestao_financeira_subtransacoes',
  'saldo_conta',
] as const;

export type NomeDataset = (typeof NOMES_DATASETS)[number];

export type DadosModulo = Partial<Record<NomeDataset, TransferRecord[]>>;

export type TipoGrafico = 'barras' | 'rosca' | 'anos';

export type GraficoModulo = {
  id: string;
  titulo: string;
  dica: string;
  tipo: TipoGrafico;
  dataset: NomeDataset;
  /** Coluna que agrupa: categoria no ranking/rosca, ano no gráfico por ano. */
  agrupar: string;
  /** Coluna somada. Sem ela, o gráfico mostra quantidade de registros. */
  somar?: string;
  /** Quantas categorias mostrar no ranking (o restante fica de fora). */
  limite?: number;
};

export type GrupoModulo = {
  id: string;
  titulo: string;
  descricao: string;
  graficos: GraficoModulo[];
};

export const GRUPOS_MODULO: GrupoModulo[] = [
  {
    id: 'planos-acao',
    titulo: 'Planos de ação',
    descricao: 'Planos de ação do Tocantins: situação, ano, beneficiário e área de política pública.',
    graficos: [
      {
        id: 'planos-situacao',
        titulo: 'Planos por situação',
        dica: 'Quantidade de planos de ação',
        tipo: 'rosca',
        dataset: 'planos_acao',
        agrupar: 'situacao_plano_acao',
      },
      {
        id: 'planos-ano',
        titulo: 'Planos por ano',
        dica: 'Valor total (custeio + investimento) e quantidade, por ano do plano',
        tipo: 'anos',
        dataset: 'planos_acao',
        agrupar: 'ano_plano_acao',
        somar: 'valor_total',
      },
      {
        id: 'planos-beneficiario',
        titulo: 'Beneficiários com maior valor',
        dica: '10 maiores, por valor total dos planos',
        tipo: 'barras',
        dataset: 'planos_acao',
        agrupar: 'nome_beneficiario',
        somar: 'valor_total',
        limite: 10,
      },
      {
        id: 'planos-area',
        titulo: 'Áreas de política pública',
        dica: 'Valor total dos planos por área, 10 maiores',
        tipo: 'barras',
        dataset: 'planos_acao',
        agrupar: 'codigo_descricao_areas_politicas_publicas_plano_acao',
        somar: 'valor_total',
        limite: 10,
      },
      {
        id: 'planos-categoria',
        titulo: 'Categoria de despesa',
        dica: 'Quantidade de planos por categoria (custeio ou investimento)',
        tipo: 'rosca',
        dataset: 'planos_acao',
        agrupar: 'categoria_despesa_plano_acao',
      },
    ],
  },
  {
    id: 'execucao',
    titulo: 'Execução orçamentária',
    descricao: 'Empenhos, documentos hábeis, ordens de pagamento e devoluções.',
    graficos: [
      {
        id: 'empenhos-situacao',
        titulo: 'Empenhos por situação',
        dica: 'Quantidade de empenhos por situação',
        tipo: 'rosca',
        dataset: 'empenhos',
        agrupar: 'descricao_situacao_empenho',
      },
      {
        id: 'empenhos-ano',
        titulo: 'Empenhos por ano de emissão',
        dica: 'Valor empenhado e quantidade, por ano de emissão',
        tipo: 'anos',
        dataset: 'empenhos',
        agrupar: 'ano_emissao',
        somar: 'valor_empenho',
      },
      {
        id: 'dh-situacao',
        titulo: 'Documentos hábeis por situação',
        dica: 'Valor dos DH por situação, 10 maiores',
        tipo: 'barras',
        dataset: 'documentos_habeis',
        agrupar: 'descricao_situacao_dh',
        somar: 'valor_dh',
        limite: 10,
      },
      {
        id: 'ob-situacao',
        titulo: 'Ordens de pagamento por situação',
        dica: 'Quantidade de ordens por situação',
        tipo: 'rosca',
        dataset: 'ordens_pagamento',
        agrupar: 'descricao_situacao_op',
      },
      {
        id: 'devolucoes-motivo',
        titulo: 'Devoluções por motivo',
        dica: 'Valor total devolvido, por motivo',
        tipo: 'barras',
        dataset: 'devolucao',
        agrupar: 'ds_motivo_devolucao',
        somar: 'vl_total_devolucao',
        limite: 10,
      },
    ],
  },
  {
    id: 'gestao-financeira',
    titulo: 'Gestão financeira',
    descricao: 'Lançamentos e subtransações da conta específica das transferências.',
    graficos: [
      {
        id: 'lancamentos-operacao',
        titulo: 'Lançamentos por tipo de operação',
        dica: 'Quantidade de lançamentos por tipo de operação',
        tipo: 'rosca',
        dataset: 'gestao_financeira_lancamentos',
        agrupar: 'tipo_operacao_gestao_financeira',
      },
      {
        id: 'lancamentos-ano',
        titulo: 'Lançamentos por ano',
        dica: 'Valor e quantidade de lançamentos, por ano',
        tipo: 'anos',
        dataset: 'gestao_financeira_lancamentos',
        agrupar: 'ano_lancamento',
        somar: 'valor_gestao_financeira',
      },
      {
        id: 'subtransacoes-situacao',
        titulo: 'Subtransações por situação de pagamento',
        dica: 'Quantidade de subtransações por situação de pagamento',
        tipo: 'rosca',
        dataset: 'gestao_financeira_subtransacoes',
        agrupar: 'situacao_pagamento_subtransacao_gestao_financeira',
      },
      {
        id: 'subtransacoes-favorecido',
        titulo: 'Favorecidos com maior valor',
        dica: '10 maiores favorecidos, por valor das subtransações',
        tipo: 'barras',
        dataset: 'gestao_financeira_subtransacoes',
        agrupar: 'nome_beneficiario_subtransacao_gestao_financeira',
        somar: 'valor_subtransacao_gestao_financeira',
        limite: 10,
      },
    ],
  },
  {
    id: 'executores',
    titulo: 'Executores e metas',
    descricao: 'Quem executa os planos, em quais áreas e com quais metas.',
    graficos: [
      {
        id: 'executores-top',
        titulo: 'Executores com maior valor',
        dica: '10 maiores executores, por custeio + investimento',
        tipo: 'barras',
        dataset: 'executores',
        agrupar: 'nome_executor',
        somar: 'valor_executor',
        limite: 10,
      },
      {
        id: 'finalidade-area',
        titulo: 'Finalidade por área de política pública',
        dica: 'Quantidade de registros de finalidade por área',
        tipo: 'rosca',
        dataset: 'finalidade',
        agrupar: 'area_politica_publica_pt',
      },
      {
        id: 'metas-unidade',
        titulo: 'Metas por unidade de medida',
        dica: 'Valor total das metas, por unidade de medida',
        tipo: 'barras',
        dataset: 'meta',
        agrupar: 'un_medida_meta',
        somar: 'valor_meta',
        limite: 10,
      },
    ],
  },
  {
    id: 'planos-trabalho',
    titulo: 'Planos de trabalho e análises',
    descricao: 'Situação dos planos de trabalho, pareceres e órgãos com análises pendentes.',
    graficos: [
      {
        id: 'pt-situacao',
        titulo: 'Planos de trabalho por situação',
        dica: 'Quantidade de planos de trabalho',
        tipo: 'rosca',
        dataset: 'planos_trabalho',
        agrupar: 'situacao_plano_trabalho',
      },
      {
        id: 'analises-parecer',
        titulo: 'Análises por situação do parecer',
        dica: 'Quantidade de análises por situação do parecer',
        tipo: 'rosca',
        dataset: 'planos_trabalho_analises',
        agrupar: 'situacao_parecer_analise_pt',
      },
      {
        id: 'analises-orgao',
        titulo: 'Órgãos que mais analisam',
        dica: '10 órgãos com mais análises',
        tipo: 'barras',
        dataset: 'planos_trabalho_analises',
        agrupar: 'nome_orgao_analise_pt',
        limite: 10,
      },
      {
        id: 'pendentes-orgao',
        titulo: 'Órgãos com análises pendentes',
        dica: '10 órgãos com mais planos de trabalho pendentes',
        tipo: 'barras',
        dataset: 'orgaos_analises_pendentes',
        agrupar: 'nome_orgao_analise_pendente_pt',
        limite: 10,
      },
    ],
  },
  {
    id: 'relatorios',
    titulo: 'Relatórios de gestão',
    descricao: 'Relatórios de gestão, com o valor executado e os documentos de liquidação.',
    graficos: [
      {
        id: 'relatorios-situacao',
        titulo: 'Relatórios por situação',
        dica: 'Quantidade de relatórios por situação',
        tipo: 'rosca',
        dataset: 'relatorios_gestao_novos',
        agrupar: 'situacao_relatorio_gestao_novo',
      },
      {
        id: 'liquidacao-natureza',
        titulo: 'Liquidação por natureza de despesa',
        dica: 'Valor pago, por natureza, 10 maiores',
        tipo: 'barras',
        dataset: 'relatorios_gestao_documento_liquidacao',
        agrupar: 'tx_natureza_relatorio_gestao_dl',
        somar: 'vl_pagamento_relatorio_gestao_dl',
        limite: 10,
      },
      {
        id: 'liquidacao-recebedor',
        titulo: 'Recebedores com maior pagamento',
        dica: '10 maiores recebedores, por valor pago',
        tipo: 'barras',
        dataset: 'relatorios_gestao_documento_liquidacao',
        agrupar: 'tx_nome_recebedor_relatorio_gestao_dl',
        somar: 'vl_pagamento_relatorio_gestao_dl',
        limite: 10,
      },
    ],
  },
];

export type IndicadoresModulo = {
  planos: number;
  valorPlanos: number;
  empenhado: number;
  valorDh: number;
  valorLancamentos: number;
  saldoAtual: number;
  ultimaAtualizacao?: string;
};

export type ResultadoCarga = {
  dados: DadosModulo;
  geradoEm?: string;
};

async function lerCsv(nome: NomeDataset): Promise<TransferRecord[]> {
  const resposta = await fetch(`${BASE_TRANSF_ESPECIAIS}/${nome}.csv`);
  if (!resposta.ok) {
    throw new Error(`Não foi possível carregar ${nome}.csv. Rode a coleta e sincronize os dados.`);
  }
  const texto = await resposta.text();
  return texto.trim() ? parseCsv(texto) : [];
}

async function lerGeradoEm(): Promise<string | undefined> {
  try {
    const resposta = await fetch(`${BASE_TRANSF_ESPECIAIS}/_manifest.json`);
    if (!resposta.ok) return undefined;
    const manifesto = (await resposta.json()) as { gerado_em?: string };
    return manifesto.gerado_em;
  } catch {
    return undefined;
  }
}

/** Carrega todos os datasets do módulo em paralelo e aplica as derivações. */
export async function carregarModulo(): Promise<ResultadoCarga> {
  const [linhas, geradoEm] = await Promise.all([
    Promise.all(NOMES_DATASETS.map((nome) => lerCsv(nome).then((rows) => [nome, rows] as const))),
    lerGeradoEm(),
  ]);

  const dados = Object.fromEntries(linhas) as DadosModulo;
  aplicarDerivacoes(dados);
  return { dados, geradoEm };
}

/**
 * Colunas que não existem no CSV bruto e que os gráficos precisam.
 * Feito uma vez, aqui, para que os componentes continuem genéricos.
 */
export function aplicarDerivacoes(dados: DadosModulo) {
  const nomesBeneficiarios = new Map(
    (dados.beneficiarios ?? []).map((b) => [String(b.id_beneficiario), String(b.nome_beneficiario ?? '')]),
  );

  for (const linhas of Object.values(dados)) {
    for (const linha of linhas ?? []) {
      linha[COLUNA_REGISTRO] = 1;
    }
  }

  for (const plano of dados.planos_acao ?? []) {
    plano.valor_total = toNumber(plano.valor_custeio_plano_acao) + toNumber(plano.valor_investimento_plano_acao);
    plano.nome_beneficiario = nomesBeneficiarios.get(String(plano.id_beneficiario)) || 'Não informado';
  }

  for (const empenho of dados.empenhos ?? []) {
    empenho.ano_emissao = String(empenho.data_emissao_empenho ?? '').slice(0, 4);
  }

  for (const lancamento of dados.gestao_financeira_lancamentos ?? []) {
    lancamento.ano_lancamento = String(lancamento.data_lancamento_gestao_financeira ?? '').slice(0, 4);
  }

  for (const executor of dados.executores ?? []) {
    executor.valor_executor = toNumber(executor.vl_custeio_executor) + toNumber(executor.vl_investimento_executor);
  }

  const colunasMeta = [
    'vl_custeio_emenda_especial_meta',
    'vl_investimento_emenda_especial_meta',
    'vl_custeio_recursos_proprios_meta',
    'vl_investimento_recursos_proprios_meta',
    'vl_custeio_rendimento_meta',
    'vl_investimento_rendimento_meta',
    'vl_custeio_doacao_meta',
    'vl_investimento_doacao_meta',
  ];
  for (const meta of dados.meta ?? []) {
    meta.valor_meta = colunasMeta.reduce((total, coluna) => total + toNumber(meta[coluna]), 0);
  }
}

/** Indicadores do topo da página. O saldo usa só o registro mais recente de cada conta. */
export function calcularIndicadores(dados: DadosModulo): IndicadoresModulo {
  const soma = (linhas: TransferRecord[] | undefined, coluna: string) =>
    (linhas ?? []).reduce((total, linha) => total + toNumber(linha[coluna]), 0);

  const saldoMaisRecente = new Map<string, TransferRecord>();
  for (const saldo of dados.saldo_conta ?? []) {
    const conta = String(saldo.id_agencia_conta ?? '');
    const atual = saldoMaisRecente.get(conta);
    if (!atual || String(saldo.data_saldo_conta ?? '') > String(atual.data_saldo_conta ?? '')) {
      saldoMaisRecente.set(conta, saldo);
    }
  }

  return {
    planos: dados.planos_acao?.length ?? 0,
    valorPlanos: soma(dados.planos_acao, 'valor_total'),
    empenhado: soma(dados.empenhos, 'valor_empenho'),
    valorDh: soma(dados.documentos_habeis, 'valor_dh'),
    valorLancamentos: soma(dados.gestao_financeira_lancamentos, 'valor_gestao_financeira'),
    saldoAtual: [...saldoMaisRecente.values()].reduce(
      (total, saldo) => total + toNumber(saldo.saldo_final_gestao_financeira),
      0,
    ),
    ultimaAtualizacao: dados.data_atualizacao?.[0]?.data_ultima_atualizacao as string | undefined,
  };
}

export type FiltroPlanilha = { coluna: string; rotulo: string };

/**
 * Filtros de múltipla escolha da planilha, por conjunto de dados.
 * Mesmo padrão do painel de especiais: cada filtro mostra as opções presentes nos dados.
 */
export const FILTROS_PLANILHA: Partial<Record<NomeDataset, FiltroPlanilha[]>> = {
  planos_acao: [
    { coluna: 'ano_plano_acao', rotulo: 'Ano' },
    { coluna: 'situacao_plano_acao', rotulo: 'Situação' },
    { coluna: 'nome_beneficiario', rotulo: 'Beneficiário' },
    { coluna: 'categoria_despesa_plano_acao', rotulo: 'Categoria de despesa' },
    { coluna: 'nome_parlamentar_emenda_plano_acao', rotulo: 'Parlamentar' },
  ],
  empenhos: [
    { coluna: 'ano_emissao', rotulo: 'Ano de emissão' },
    { coluna: 'descricao_situacao_empenho', rotulo: 'Situação' },
  ],
  documentos_habeis: [{ coluna: 'descricao_situacao_dh', rotulo: 'Situação' }],
  ordens_pagamento: [{ coluna: 'descricao_situacao_op', rotulo: 'Situação' }],
  gestao_financeira_lancamentos: [
    { coluna: 'ano_lancamento', rotulo: 'Ano' },
    { coluna: 'tipo_operacao_gestao_financeira', rotulo: 'Tipo de operação' },
  ],
  gestao_financeira_subtransacoes: [
    { coluna: 'situacao_pagamento_subtransacao_gestao_financeira', rotulo: 'Situação do pagamento' },
  ],
  planos_trabalho: [{ coluna: 'situacao_plano_trabalho', rotulo: 'Situação' }],
  planos_trabalho_analises: [{ coluna: 'situacao_parecer_analise_pt', rotulo: 'Situação do parecer' }],
  relatorios_gestao_novos: [{ coluna: 'situacao_relatorio_gestao_novo', rotulo: 'Situação' }],
  devolucao: [{ coluna: 'situacao_devolucao', rotulo: 'Situação' }],
  executores: [{ coluna: 'nome_executor', rotulo: 'Executor' }],
};

/** Texto de uma célula como aparece na tela e no filtro (vazio vira "Não informado"). */
export function textoCelula(valor: unknown) {
  if (valor === undefined || valor === null || valor === '' || valor === 'nan') return 'Não informado';
  return String(valor).trim();
}

/** Mantém só as linhas que batem com todos os filtros ativos (coluna -> valores escolhidos). */
export function aplicarFiltros(linhas: TransferRecord[], filtros: Record<string, string[]> | undefined) {
  const ativos = Object.entries(filtros ?? {}).filter(([, valores]) => valores.length > 0);
  if (!ativos.length) return linhas;
  return linhas.filter((linha) => ativos.every(([coluna, valores]) => valores.includes(textoCelula(linha[coluna]))));
}

export type FiltrosPorConjunto = Partial<Record<NomeDataset, Record<string, string[]>>>;

/**
 * Relações entre conjuntos por chave (pai -> filho). Validadas nos CSVs: todo filho
 * tem pai correspondente. Ex.: cada empenho pertence a um plano de ação.
 *
 * Não usamos colunas de mesmo nome: "situação" de plano e "situação" de empenho são
 * coisas diferentes. A ligação é sempre pela chave.
 *
 * Excluídas por não ligarem dados (cobertura medida):
 * - planos_trabalho_analises -> planos_trabalho_analise_historico (0%)
 * - gestao_financeira_lancamentos -> gestao_financeira_subtransacoes (0,7%)
 */
export const RELACOES: Array<{
  pai: NomeDataset;
  colunaPai: string;
  filho: NomeDataset;
  colunaFilho: string;
}> = [
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'empenhos', colunaFilho: 'id_plano_acao' },
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'executores', colunaFilho: 'id_plano_acao' },
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'devolucao', colunaFilho: 'id_plano_acao' },
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'planos_trabalho', colunaFilho: 'id_plano_acao' },
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'relatorios_gestao', colunaFilho: 'id_plano_acao' },
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'relatorios_gestao_novos', colunaFilho: 'id_plano_acao' },
  { pai: 'planos_acao', colunaPai: 'id_plano_acao', filho: 'planos_acao_historico', colunaFilho: 'id_plano_acao' },
  { pai: 'beneficiarios', colunaPai: 'id_beneficiario', filho: 'planos_acao', colunaFilho: 'id_beneficiario' },
  { pai: 'planos_acao', colunaPai: 'id_agencia_conta', filho: 'saldo_conta', colunaFilho: 'id_agencia_conta' },
  { pai: 'planos_acao', colunaPai: 'id_agencia_conta', filho: 'gestao_financeira_lancamentos', colunaFilho: 'id_agencia_conta' },
  { pai: 'empenhos', colunaPai: 'id_empenho', filho: 'documentos_habeis', colunaFilho: 'id_empenho' },
  { pai: 'documentos_habeis', colunaPai: 'id_dh', filho: 'ordens_pagamento', colunaFilho: 'id_dh' },
  { pai: 'executores', colunaPai: 'id_executor', filho: 'meta', colunaFilho: 'id_executor' },
  { pai: 'executores', colunaPai: 'id_executor', filho: 'finalidade', colunaFilho: 'id_executor' },
  { pai: 'planos_trabalho', colunaPai: 'id_plano_trabalho', filho: 'planos_trabalho_analises', colunaFilho: 'id_plano_trabalho' },
  { pai: 'planos_trabalho', colunaPai: 'id_plano_trabalho', filho: 'planos_trabalho_historico', colunaFilho: 'id_plano_trabalho' },
  { pai: 'planos_trabalho', colunaPai: 'id_plano_trabalho', filho: 'orgaos_analises_pendentes', colunaFilho: 'id_plano_trabalho' },
  { pai: 'relatorios_gestao_novos', colunaPai: 'id_relatorio_gestao_novo', filho: 'relatorios_gestao_documento_liquidacao', colunaFilho: 'id_relatorio_gestao_novo' },
];

/** Valor de chave como texto; vazio não é chave (não deve casar com outro vazio). */
function chaveDe(valor: unknown): string | null {
  if (valor === undefined || valor === null || valor === '' || valor === 'nan') return null;
  return String(valor).trim();
}

/**
 * Aplica os filtros e propaga pelas relações, em duas fases:
 *
 * 1. Para cima: a partir dos conjuntos filtrados diretamente, cada pai ligado por chave
 *    mantém só os registros que têm filho no filtro. Ex.: "OB enviada" -> documentos -> empenhos -> planos.
 * 2. Para baixo: a partir de tudo que ficou restrito, cada filho mantém só os registros
 *    cujo pai sobrou. Ex.: planos CIENTE -> empenhos, executores, devolução, planos de trabalho.
 *
 * A fase 2 não sobe de volta. Sem essa regra, um conjunto pequeno (ex.: 5 devoluções)
 * restringiria os planos a 5, zerando o restante.
 */
export function filtrarDados(dados: DadosModulo, filtros: FiltrosPorConjunto): DadosModulo {
  const atual: DadosModulo = { ...dados };
  const restritos = new Set<NomeDataset>();

  for (const nome of NOMES_DATASETS) {
    if (temFiltroAtivo(filtros[nome])) {
      atual[nome] = aplicarFiltros(dados[nome] ?? [], filtros[nome]);
      restritos.add(nome);
    }
  }

  // Fila de trabalho: só reprocessa as relações de um conjunto quando ele encolheu.
  // Mesmo resultado de varrer todas as relações até estabilizar, mas sem refazer o trabalho.

  // Fase 1: para cima, a partir dos filtrados diretamente.
  const fila: NomeDataset[] = [...restritos];
  while (fila.length > 0) {
    const origem = fila.shift() as NomeDataset;
    for (const relacao of RELACOES) {
      if (relacao.filho !== origem) continue;
      if (restringir(atual, restritos, origem, relacao.colunaFilho, relacao.pai, relacao.colunaPai)) {
        fila.push(relacao.pai);
      }
    }
  }

  // Fase 2: para baixo, a partir de tudo que ficou restrito. Não sobe de volta.
  const filaBaixo: NomeDataset[] = [...restritos];
  while (filaBaixo.length > 0) {
    const origem = filaBaixo.shift() as NomeDataset;
    for (const relacao of RELACOES) {
      if (relacao.pai !== origem) continue;
      if (restringir(atual, restritos, origem, relacao.colunaPai, relacao.filho, relacao.colunaFilho)) {
        filaBaixo.push(relacao.filho);
      }
    }
  }

  return atual;
}

/**
 * Mantém em `destino` só as linhas cuja chave aparece em `origem`. Marca `destino` como
 * restrito. Devolve true quando `destino` ganhou restrição ou perdeu linhas.
 */
function restringir(
  atual: DadosModulo,
  restritos: Set<NomeDataset>,
  origem: NomeDataset,
  colunaOrigem: string,
  destino: NomeDataset,
  colunaDestino: string,
): boolean {
  const chaves = new Set(
    (atual[origem] ?? []).map((linha) => chaveDe(linha[colunaOrigem])).filter((c): c is string => c !== null),
  );
  const linhas = atual[destino] ?? [];
  const filtradas = linhas.filter((linha) => {
    const chave = chaveDe(linha[colunaDestino]);
    return chave !== null && chaves.has(chave);
  });

  const novo = !restritos.has(destino);
  restritos.add(destino);
  if (novo || filtradas.length !== linhas.length) {
    atual[destino] = filtradas;
    return true;
  }
  return false;
}

function temFiltroAtivo(filtro: Record<string, string[]> | undefined) {
  return Object.values(filtro ?? {}).some((valores) => valores.length > 0);
}
