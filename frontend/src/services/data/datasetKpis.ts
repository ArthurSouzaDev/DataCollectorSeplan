import { formatNumber, toNumber, type DatasetConfig, type TransferRecord } from './transferData';

/**
 * KPIs por dataset.
 *
 * As fórmulas das discricionárias foram portadas de `backend/app_discricionarias.py`
 * (bloco "KPIs conforme definição da tela de referência"). Mantê-las isoladas aqui
 * permite que o DatasetDetailPage continue genérico: ele apenas renderiza a lista
 * que esta camada devolve.
 *
 * Os itens carregam o valor bruto — quem formata é o componente, que decide entre
 * o valor por extenso e a forma abreviada conforme a quantidade de cartões na linha.
 */

export type KpiTone = 'accent' | 'success' | 'warm' | 'danger';

export type KpiItem = {
  key: string;
  label: string;
  amount: number;
  kind: 'currency' | 'count';
  tone?: KpiTone;
  /** Texto auxiliar abaixo do valor (ex.: percentual de execução). */
  hint?: string;
};

export function sumColumn(rows: TransferRecord[], column: string) {
  let total = 0;
  for (const row of rows) total += toNumber(row[column]);
  return total;
}

/** Soma descartando valores negativos linha a linha (equivale ao `.clip(lower=0)` do pandas). */
function sumColumnClamped(rows: TransferRecord[], column: string) {
  let total = 0;
  for (const row of rows) total += Math.max(toNumber(row[column]), 0);
  return total;
}

function sumColumns(rows: TransferRecord[], columns: string[]) {
  let total = 0;
  for (const row of rows) {
    for (const column of columns) total += toNumber(row[column]);
  }
  return total;
}

export function computeKpis(config: DatasetConfig, rows: TransferRecord[]): KpiItem[] {
  if (config.id === 'discricionarias-legais') return discricionariasKpis(rows);
  if (config.id === 'especiais') return especiaisKpis(rows);
  return fundoAFundoKpis(rows);
}

function discricionariasKpis(rows: TransferRecord[]): KpiItem[] {
  // Valor Global = repasse + contrapartida
  const valorGlobal = sumColumns(rows, ['valor_repasse', 'valor_contrapartida']);

  // Valor Liberado = desembolsado + ingresso de contrapartida + rendimento de aplicação
  const valorLiberado = sumColumns(rows, [
    'valor_desembolsado',
    'vl_ingresso_contrapartida',
    'vl_rendimento_aplicacao',
  ]);

  // Saldo em conta nunca é negativo: descarta outliers negativos do SICONV.
  const saldoConta = sumColumnClamped(rows, 'valor_saldo_conta');

  // Devolvidos = saldo remanescente devolvido ao tesouro + devolvido pelo convenente
  const devolvidos = sumColumns(rows, ['valor_saldo_tesouro', 'vl_saldo_reman_convenente']);

  const contrapartida = sumColumn(rows, 'valor_contrapartida');
  const valorPago = sumColumn(rows, 'valor_pago');
  const valorRepasse = sumColumn(rows, 'valor_repasse');

  // A base junta propostas e convênios: só parte das propostas do Tocantins
  // chega a virar convênio, então contar linhas e chamar tudo de "convênio"
  // infla o número em ~3x. A coluna `fase` (vinda do coletor) separa os dois.
  const convenios = rows.reduce(
    (total, row) => total + (String(row.fase ?? '').trim() === 'Convênio' ? 1 : 0),
    0,
  );
  const temFase = rows.some((row) => row.fase !== undefined);

  return [
    {
      key: 'registros',
      label: temFase ? 'Registros' : 'Convênios',
      amount: rows.length,
      kind: 'count',
      tone: 'accent',
      hint: temFase
        ? `${formatNumber(convenios)} convênios · ${formatNumber(rows.length - convenios)} propostas`
        : undefined,
    },
    { key: 'global', label: 'Valor global', amount: valorGlobal, kind: 'currency', tone: 'accent' },
    {
      key: 'liberado',
      label: 'Valor liberado',
      amount: valorLiberado,
      kind: 'currency',
      tone: 'success',
      hint: percentOf(valorLiberado, valorGlobal, 'do valor global'),
    },
    { key: 'contrapartida', label: 'Contrapartida', amount: contrapartida, kind: 'currency' },
    { key: 'saldo', label: 'Saldo em conta', amount: saldoConta, kind: 'currency', tone: 'warm' },
    { key: 'devolvidos', label: 'Valores devolvidos', amount: devolvidos, kind: 'currency', tone: 'danger' },
    {
      key: 'pago',
      label: 'Valor pago',
      amount: valorPago,
      kind: 'currency',
      tone: 'success',
      hint: percentOf(valorPago, valorRepasse, 'do repasse'),
    },
  ];
}

function especiaisKpis(rows: TransferRecord[]): KpiItem[] {
  const custeio = sumColumn(rows, 'valor_custeio');
  const investimento = sumColumn(rows, 'valor_investimento');
  const total = custeio + investimento;

  return [
    { key: 'planos', label: 'Total de especiais', amount: rows.length, kind: 'count', tone: 'accent' },
    { key: 'total', label: 'Valor total', amount: total, kind: 'currency', tone: 'accent' },
    {
      key: 'custeio',
      label: 'Custeio',
      amount: custeio,
      kind: 'currency',
      tone: 'success',
      hint: percentOf(custeio, total, 'do total'),
    },
    {
      key: 'investimento',
      label: 'Investimento',
      amount: investimento,
      kind: 'currency',
      tone: 'warm',
      hint: percentOf(investimento, total, 'do total'),
    },
  ];
}

function fundoAFundoKpis(rows: TransferRecord[]): KpiItem[] {
  const totalRepasse = sumColumn(rows, 'valor_total_repasse');
  const emenda = sumColumn(rows, 'valor_emenda');

  // `saldo_disponivel` não entra como KPI: na base atual ela é sempre
  // valor_total_plano − valor_total_repasse, ou seja, resíduo de ponto
  // flutuante (~1e-9) em 591 dos 608 registros.
  return [
    { key: 'planos', label: 'Planos', amount: rows.length, kind: 'count', tone: 'accent' },
    { key: 'repasse', label: 'Total repasse', amount: totalRepasse, kind: 'currency', tone: 'accent' },
    {
      key: 'emenda',
      label: 'Via emenda',
      amount: emenda,
      kind: 'currency',
      tone: 'success',
      hint: percentOf(emenda, totalRepasse, 'do repasse'),
    },
    { key: 'especifico', label: 'Específico', amount: sumColumn(rows, 'valor_especifico'), kind: 'currency' },
    { key: 'voluntario', label: 'Voluntário', amount: sumColumn(rows, 'valor_voluntario'), kind: 'currency' },
    {
      key: 'plano',
      label: 'Valor total dos planos',
      amount: sumColumn(rows, 'valor_total_plano'),
      kind: 'currency',
      tone: 'warm',
    },
  ];
}

function percentOf(part: number, whole: number, suffix: string) {
  if (whole <= 0) return undefined;
  return `${formatPercent(( part / whole) * 100)} ${suffix}`;
}

export function formatPercent(value: number) {
  return `${value.toFixed(1).replace('.', ',')}%`;
}

/* ── Execução financeira (discricionárias) ─────────────────────────────────── */

export type ExecutionStage = {
  key: 'repasse' | 'desembolsado' | 'pago';
  label: string;
  value: number;
  /** Percentual sobre o valor de repasse (etapa inicial do funil). */
  percentOfInitial: number;
};

export type ExecutionSummary = {
  stages: ExecutionStage[];
  percentDisbursed: number;
  percentPaid: number;
  saldoConta: number;
};

export function computeExecution(rows: TransferRecord[]): ExecutionSummary {
  const repasse = sumColumn(rows, 'valor_repasse');
  const desembolsado = sumColumn(rows, 'valor_desembolsado');
  const pago = sumColumn(rows, 'valor_pago');

  const share = (value: number) => (repasse > 0 ? (value / repasse) * 100 : 0);

  return {
    stages: [
      { key: 'repasse', label: 'Valor repasse', value: repasse, percentOfInitial: repasse > 0 ? 100 : 0 },
      { key: 'desembolsado', label: 'Valor desembolsado', value: desembolsado, percentOfInitial: share(desembolsado) },
      { key: 'pago', label: 'Valor pago', value: pago, percentOfInitial: share(pago) },
    ],
    percentDisbursed: share(desembolsado),
    percentPaid: share(pago),
    saldoConta: sumColumnClamped(rows, 'valor_saldo_conta'),
  };
}

export function supportsExecutionPanel(config: DatasetConfig) {
  return config.id === 'discricionarias-legais';
}
