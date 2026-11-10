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
