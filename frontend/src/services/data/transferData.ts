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
};

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

export function uniqueOptions(rows: TransferRecord[], column: string) {
  return Array.from(new Set(rows.map((row) => cleanLabel(row[column])).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b),
  );
}

export function filterRows(
  rows: TransferRecord[],
  filters: { years?: string[]; statuses?: string[]; groups?: string[]; search?: string },
  config: DatasetConfig,
) {
  const term = filters.search?.trim().toLowerCase();
  const yearSet = new Set(filters.years ?? []);
  const statusSet = new Set(filters.statuses ?? []);
  const groupSet = new Set(filters.groups ?? []);

  return rows.filter((row) => {
    if (yearSet.size > 0 && !yearSet.has(cleanLabel(row[config.yearColumn]))) return false;
    if (statusSet.size > 0 && !statusSet.has(cleanLabel(row[config.statusColumn]))) return false;
    if (groupSet.size > 0 && !groupSet.has(cleanLabel(row[config.groupColumn]))) return false;
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
