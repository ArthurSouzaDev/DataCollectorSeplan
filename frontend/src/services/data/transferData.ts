export type DatasetId = 'especiais' | 'discricionarias-legais' | 'fundo-a-fundo';

export type TransferRecord = Record<string, string | number>;

export type DatasetConfig = {
  id: DatasetId;
  title: string;
  shortTitle: string;
  description: string;
  source: string;
  dataUrl: string;
  valueColumn: string;
  countLabel: string;
  valueLabel: string;
  entityColumn: string;
  groupColumn: string;
  yearColumn: string;
  statusColumn: string;
  tableColumns: string[];
  /** Decomposição do valor em partes (custeio × investimento, tipo de repasse…). */
  composition?: CompositionConfig;
};

export type CompositionSeries = { key: string; label: string };

export type CompositionConfig = {
  title: string;
  hint: string;
  groupColumn: string;
  series: CompositionSeries[];
  /** Quantos grupos exibir (os demais viram "Outros"). */
  limit?: number;
};

export type CompositionRow = { label: string; total: number } & Record<string, number | string>;

export type DatasetSummary = {
  totalRecords: number;
  totalValue: number;
  secondaryValue: number;
  yearCount: number;
  topEntities: AggregateRow[];
  statusRows: AggregateRow[];
  yearRows: AggregateRow[];
};

export type AggregateRow = {
  label: string;
  value: number;
  count: number;
};

export const DATASETS: DatasetConfig[] = [
  {
    id: 'especiais',
    title: 'Transferencias Especiais',
    shortTitle: 'Especiais',
    description: 'Emendas parlamentares especiais destinadas ao Tocantins.',
    source: 'Transferegov — Emendas Parlamentares Especiais',
    dataUrl: '/data/especiais.csv',
    valueColumn: 'valor_total',
    countLabel: 'Planos',
    valueLabel: 'Valor total',
    entityColumn: 'beneficiario',
    groupColumn: 'parlamentar',
    yearColumn: 'ano_emenda',
    statusColumn: 'situacao',
    tableColumns: [
      'codigo_plano',
      'ano_emenda',
      'parlamentar',
      'beneficiario',
      'situacao',
      'valor_custeio',
      'valor_investimento',
      'valor_total',
      'natureza_juridica',
    ],
    composition: {
      title: 'Custeio × investimento por ano',
      hint: 'Composição do valor total',
      groupColumn: 'ano_emenda',
      series: [
        { key: 'valor_custeio', label: 'Custeio' },
        { key: 'valor_investimento', label: 'Investimento' },
      ],
    },
  },
  {
    id: 'discricionarias-legais',
    title: 'Discricionarias e Legais',
    shortTitle: 'Discricionarias e Legais',
    description: 'Convenios e propostas processados a partir dos dados SICONV.',
    source: 'SICONV — Convênios e Transferências Discricionárias',
    dataUrl: '/data/discricionarias-legais.csv',
    valueColumn: 'valor_repasse',
    countLabel: 'Convenios',
    valueLabel: 'Valor repasse',
    entityColumn: 'municipio_beneficiario',
    groupColumn: 'orgao_concedente',
    yearColumn: 'ano_assinatura',
    statusColumn: 'situacao',
    tableColumns: [
      'nr_convenio',
      'fase',
      'ano_assinatura',
      'municipio_beneficiario',
      'proponente',
      'orgao_concedente',
      'situacao',
      'valor_global',
      'valor_repasse',
      'valor_pago',
      'valor_saldo_conta',
      'natureza_juridica',
    ],
  },
  {
    id: 'fundo-a-fundo',
    title: 'Fundo a Fundo',
    shortTitle: 'Fundo a Fundo',
    description: 'Planos fundo a fundo filtrados para o estado do Tocantins.',
    source: 'Transferegov — Fundo a Fundo',
    dataUrl: '/data/fundo-a-fundo.csv',
    valueColumn: 'valor_total_repasse',
    countLabel: 'Planos',
    valueLabel: 'Total repasse',
    entityColumn: 'municipio',
    groupColumn: 'sigla_orgao',
    yearColumn: 'ano',
    statusColumn: 'situacao',
    tableColumns: [
      'codigo_plano',
      'ano',
      'municipio',
      'sigla_orgao',
      'fundo_repassador',
      'situacao',
      'valor_emenda',
      'valor_total_repasse',
      'valor_total_plano',
      'saldo_disponivel',
      'natureza_juridica',
    ],
    composition: {
      title: 'Composição do repasse por órgão',
      hint: 'Emenda, específico e voluntário',
      groupColumn: 'sigla_orgao',
      series: [
        { key: 'valor_emenda', label: 'Emenda' },
        { key: 'valor_especifico', label: 'Específico' },
        { key: 'valor_voluntario', label: 'Voluntário' },
      ],
      limit: 8,
    },
  },
];

export type DataManifest = Partial<Record<DatasetId, string>>;

const memoryCache = new Map<DatasetId, Promise<TransferRecord[]>>();
let manifestCache: Promise<DataManifest> | null = null;

export function loadManifest(): Promise<DataManifest> {
  if (!manifestCache) {
    manifestCache = fetch('/data/manifest.json')
      .then((r) => (r.ok ? (r.json() as Promise<DataManifest>) : {}))
      .catch(() => ({}));
  }
  return manifestCache;
}

export function formatLastModified(isoDate: string | undefined): string {
  if (!isoDate) return '';
  try {
    return new Intl.DateTimeFormat('pt-BR', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'America/Araguaina',
    }).format(new Date(isoDate));
  } catch {
    return '';
  }
}

export function getDatasetConfig(datasetId: DatasetId) {
  return DATASETS.find((dataset) => dataset.id === datasetId);
}

export async function loadDataset(config: DatasetConfig) {
  if (!memoryCache.has(config.id)) {
    memoryCache.set(
      config.id,
      fetch(config.dataUrl)
        .then((response) => {
          if (!response.ok) {
            throw new Error(`Nao foi possivel carregar ${config.dataUrl}`);
          }
          return response.text();
        })
        .then((text) => normalizeRows(config.id, parseCsv(text))),
    );
  }

  return memoryCache.get(config.id)!;
}

export function summarizeDataset(config: DatasetConfig, rows: TransferRecord[]): DatasetSummary {
  const totalValue = sumColumn(rows, config.valueColumn);
  const secondaryValue =
    config.id === 'especiais'
      ? sumColumn(rows, 'valor_investimento')
      : config.id === 'fundo-a-fundo'
        ? sumColumn(rows, 'saldo_disponivel')
        : sumColumn(rows, 'valor_pago');

  return {
    totalRecords: rows.length,
    totalValue,
    secondaryValue,
    yearCount: new Set(rows.map((row) => String(row[config.yearColumn] || '')).filter(Boolean)).size,
    topEntities: aggregateBy(rows, config.entityColumn, config.valueColumn, 5),
    statusRows: aggregateBy(rows, config.statusColumn, config.valueColumn, 5),
    yearRows: aggregateBy(rows, config.yearColumn, config.valueColumn, 12).sort((a, b) =>
      a.label.localeCompare(b.label),
    ),
  };
}

export function aggregateBy(
  rows: TransferRecord[],
  labelColumn: string,
  valueColumn: string,
  limit = 10,
): AggregateRow[] {
  const grouped = new Map<string, AggregateRow>();

  for (const row of rows) {
    const label = cleanLabel(row[labelColumn]);
    const current = grouped.get(label) ?? { label, value: 0, count: 0 };
    current.value += toNumber(row[valueColumn]);
    current.count += 1;
    grouped.set(label, current);
  }

  return Array.from(grouped.values())
    .sort((a, b) => b.value - a.value || b.count - a.count)
    .slice(0, limit);
}

export type ShareRow = AggregateRow & { share: number };

/**
 * Distribuição parte-todo por contagem de registros.
 *
 * Categorias abaixo de `minShare` (e tudo além de `maxSlices - 1`) são somadas em
 * "Outros", como no dashboard Streamlit. O limite existe porque uma rosca deixa de
 * ser legível acima de ~6 fatias — e porque a paleta categórica tem 6 posições.
 */
export function aggregateShare(
  rows: TransferRecord[],
  labelColumn: string,
  valueColumn: string,
  { maxSlices = 6, minShare = 2 }: { maxSlices?: number; minShare?: number } = {},
): { slices: ShareRow[]; total: number } {
  const grouped = new Map<string, AggregateRow>();

  for (const row of rows) {
    const label = cleanLabel(row[labelColumn]);
    const current = grouped.get(label) ?? { label, value: 0, count: 0 };
    current.value += toNumber(row[valueColumn]);
    current.count += 1;
    grouped.set(label, current);
  }

  const total = rows.length;
  if (!total) return { slices: [], total: 0 };

  const ordered = Array.from(grouped.values()).sort((a, b) => b.count - a.count);
  const named: ShareRow[] = [];
  const tail: AggregateRow[] = [];

  for (const item of ordered) {
    const share = (item.count / total) * 100;
    if (named.length < maxSlices - 1 && share >= minShare) named.push({ ...item, share });
    else tail.push(item);
  }

  if (tail.length) {
    const value = tail.reduce((sum, item) => sum + item.value, 0);
    const count = tail.reduce((sum, item) => sum + item.count, 0);
    named.push({ label: 'Outros', value, count, share: (count / total) * 100 });
  }

  return { slices: named, total };
}

/**
 * Decompõe o valor de cada grupo nas suas parcelas (custeio × investimento,
 * emenda × específico × voluntário). Grupos além de `limit` viram "Outros".
 */
export function aggregateComposition(rows: TransferRecord[], config: CompositionConfig): CompositionRow[] {
  const keys = config.series.map((serie) => serie.key);
  const grouped = new Map<string, CompositionRow>();

  for (const row of rows) {
    const label = cleanLabel(row[config.groupColumn]);
    let current = grouped.get(label);
    if (!current) {
      current = { label, total: 0 };
      for (const key of keys) current[key] = 0;
      grouped.set(label, current);
    }
    for (const key of keys) {
      const value = toNumber(row[key]);
      current[key] = (current[key] as number) + value;
      current.total += value;
    }
  }

  const ordered = Array.from(grouped.values()).sort((a, b) => b.total - a.total);
  if (!config.limit || ordered.length <= config.limit) {
    // Sem corte, um eixo de anos deve sair em ordem cronológica.
    return config.groupColumn.startsWith('ano')
      ? ordered.sort((a, b) => a.label.localeCompare(b.label))
      : ordered.reverse();
  }

  const head = ordered.slice(0, config.limit - 1);
  const tail = ordered.slice(config.limit - 1);
  const outros: CompositionRow = { label: 'Outros', total: 0 };
  for (const key of keys) outros[key] = 0;
  for (const item of tail) {
    for (const key of keys) outros[key] = (outros[key] as number) + (item[key] as number);
    outros.total += item.total;
  }

  return [...head, outros].reverse();
}

/** Série temporal ordenada por ano, com valor e quantidade em cada ponto. */
export function aggregateByYear(rows: TransferRecord[], yearColumn: string, valueColumn: string): AggregateRow[] {
  const grouped = new Map<string, AggregateRow>();

  for (const row of rows) {
    const label = String(row[yearColumn] ?? '').trim();
    if (!label || label === 'nan') continue;
    const current = grouped.get(label) ?? { label, value: 0, count: 0 };
    current.value += toNumber(row[valueColumn]);
    current.count += 1;
    grouped.set(label, current);
  }

  return Array.from(grouped.values()).sort((a, b) => a.label.localeCompare(b.label));
}

export function uniqueOptions(rows: TransferRecord[], column: string) {
  return Array.from(new Set(rows.map((row) => cleanLabel(row[column])).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b),
  );
}

// Antes da coluna `fase` existir, "formalizado" era inferido pelo rótulo da
// situação — um proxy que só funcionava porque as propostas chegavam sem
// situação nenhuma. Mantido como fallback para bases geradas antes da correção
// do coletor; com `fase` presente, o teste é direto.
const SITUACOES_NAO_FORMALIZADAS = new Set([
  'Nao informado',
  'Não informado',
  'Cancelado',
  'Proposta/Plano de Trabalho Aprovado',
  'Proposta/Plano de Trabalho Complementado em Análise',
]);

/** Formalizado = a proposta virou convênio e ele não foi cancelado. */
function isFormalized(row: TransferRecord, config: DatasetConfig) {
  if (row.fase !== undefined) {
    return cleanLabel(row.fase) === 'Convênio' && cleanLabel(row[config.statusColumn]) !== 'Cancelado';
  }
  return !SITUACOES_NAO_FORMALIZADAS.has(cleanLabel(row[config.statusColumn]));
}

export function filterRows(
  rows: TransferRecord[],
  filters: {
    years?: string[];
    statuses?: string[];
    groups?: string[];
    natures?: string[];
    onlyFormalized?: boolean;
    search?: string;
  },
  config: DatasetConfig,
) {
  const term = filters.search?.trim().toLowerCase();
  const yearSet = new Set(filters.years ?? []);
  const statusSet = new Set(filters.statuses ?? []);
  const groupSet = new Set(filters.groups ?? []);
  const natureSet = new Set(filters.natures ?? []);

  return rows.filter((row) => {
    if (yearSet.size > 0 && !yearSet.has(cleanLabel(row[config.yearColumn]))) return false;
    if (statusSet.size > 0 && !statusSet.has(cleanLabel(row[config.statusColumn]))) return false;
    if (groupSet.size > 0 && !groupSet.has(cleanLabel(row[config.groupColumn]))) return false;
    if (natureSet.size > 0 && !natureSet.has(cleanLabel(row.natureza_juridica))) return false;
    if (filters.onlyFormalized && !isFormalized(row, config)) return false;
    if (term) {
      const haystack = config.tableColumns.map((column) => cleanLabel(row[column])).join(' ').toLowerCase();
      if (!haystack.includes(term)) return false;
    }
    return true;
  });
}

export function buildCsv(rows: TransferRecord[], columns: string[]) {
  const header = columns.map(escapeCsvCell).join(';');
  const body = rows.map((row) => columns.map((column) => escapeCsvCell(row[column])).join(';')).join('\n');
  return `\uFEFF${header}\n${body}`;
}

export function formatCurrency(value: number) {
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatNumber(value: number) {
  return new Intl.NumberFormat('pt-BR').format(value);
}

/**
 * Versão abreviada para cartões estreitos: R$ 7,23 bi / R$ 272,9 mi / R$ 45 mil.
 * O valor por extenso continua disponível no `title` do cartão.
 */
export function formatCurrencyShort(value: number) {
  const abs = Math.abs(value);
  const short = (divisor: number, unit: string, digits: number) =>
    `R$ ${new Intl.NumberFormat('pt-BR', { maximumFractionDigits: digits }).format(value / divisor)} ${unit}`;

  if (abs >= 1_000_000_000) return short(1_000_000_000, 'bi', 2);
  if (abs >= 1_000_000) return short(1_000_000, 'mi', 1);
  if (abs >= 100_000) return short(1_000, 'mil', 0);
  return formatCurrency(value);
}

export function toNumber(value: unknown) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (typeof value !== 'string') return 0;

  const trimmed = value.trim();
  if (!trimmed) return 0;

  // Brazilian format uses comma as decimal separator: "1.234.567,89"
  // Python CSV uses dot as decimal separator: "1234567.89"
  // Detect by presence of comma: if comma exists, treat dots as thousands separators
  const normalized = trimmed.includes(',')
    ? trimmed.replace(/\./g, '').replace(',', '.')
    : trimmed;

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeRows(datasetId: DatasetId, rows: TransferRecord[]) {
  return rows.map((row) => {
    const normalized = { ...row };

    if (datasetId === 'especiais') {
      normalized.valor_total = toNumber(row.valor_custeio) + toNumber(row.valor_investimento);
    }

    if (datasetId === 'fundo-a-fundo') {
      normalized.ano = String(row.data_inicio || '').slice(0, 4);
    }

    if (datasetId === 'discricionarias-legais' && !normalized.ano_assinatura) {
      normalized.ano_assinatura = normalized.ano_proposta;
    }

    return normalized;
  });
}

function parseCsv(text: string): TransferRecord[] {
  const rows: string[][] = [];
  let field = '';
  let row: string[] = [];
  let insideQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === '"' && insideQuotes && next === '"') {
      field += '"';
      index += 1;
      continue;
    }

    if (char === '"') {
      insideQuotes = !insideQuotes;
      continue;
    }

    if (char === ';' && !insideQuotes) {
      row.push(field);
      field = '';
      continue;
    }

    if ((char === '\n' || char === '\r') && !insideQuotes) {
      if (char === '\r' && next === '\n') index += 1;
      row.push(field);
      if (row.some((item) => item.length > 0)) rows.push(row);
      row = [];
      field = '';
      continue;
    }

    field += char;
  }

  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }

  const [header = [], ...dataRows] = rows;
  const columns = header.map((column) => column.replace(/^\uFEFF/, '').trim());

  return dataRows.map((dataRow) =>
    columns.reduce<TransferRecord>((record, column, index) => {
      record[column] = dataRow[index] ?? '';
      return record;
    }, {}),
  );
}

function cleanLabel(value: unknown) {
  const label = String(value ?? '').trim();
  return label || 'Nao informado';
}

function sumColumn(rows: TransferRecord[], column: string) {
  return rows.reduce((total, row) => total + toNumber(row[column]), 0);
}

function escapeCsvCell(value: unknown) {
  const text = String(value ?? '');
  if (text.includes(';') || text.includes('"') || text.includes('\n') || text.includes('\r')) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}
