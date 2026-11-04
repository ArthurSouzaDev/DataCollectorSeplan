import { toNumber, type CompositionRow, type CompositionSeries, type TransferRecord } from './transferData';

/**
 * Indicadores de convênios (somente discricionárias).
 *
 * Regras:
 * - "Vigência" considera convênios que ainda não foram encerrados (concluídos, anulados,
 *   rescindidos, cancelados, arquivados ou excluídos ficam de fora).
 * - "Execução" = valor liberado ÷ valor de repasse. Convênios sem repasse não entram.
 * - Datas vêm do CSV como AAAA-MM-DD; a comparação é feita em dias corridos.
 */

const SITUACOES_ENCERRADAS = new Set([
  'CONCLUIDO',
  'CONVENIO ANULADO',
  'CANCELADO',
  'RESCINDIDO',
  'ARQUIVADO',
  'EXCLUIDO',
]);

export const JANELAS_VIGENCIA = [30, 90, 180] as const;
export const LIMITE_EXECUCAO_BAIXA = 0.5;

function normalizar(valor: unknown) {
  return String(valor ?? '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .trim()
    .toUpperCase();
}

function ehConvenio(row: TransferRecord) {
  return String(row.fase ?? '').trim() === 'Convênio';
}

function encerrado(row: TransferRecord) {
  return SITUACOES_ENCERRADAS.has(normalizar(row.situacao_convenio));
}

function dataFim(row: TransferRecord): Date | null {
  const texto = String(row.data_final_vigencia ?? '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(texto)) return null;
  const [ano, mes, dia] = texto.split('-').map(Number);
  return new Date(ano, mes - 1, dia);
}

function diasEntre(data: Date, hoje: Date) {
  const inicio = new Date(hoje.getFullYear(), hoje.getMonth(), hoje.getDate());
  return Math.round((data.getTime() - inicio.getTime()) / 86_400_000);
}

export type ConvenioVigencia = {
  row: TransferRecord;
  fim: Date;
  dias: number;
  repasse: number;
  liberado: number;
  /** Percentual liberado (0–1). null quando não há repasse. */
  execucao: number | null;
};

/** Convênios ainda em curso com vigência que termina nos próximos `janela` dias, do mais urgente ao menos. */
export function vigenciasProximas(rows: TransferRecord[], hoje = new Date(), janela = 180): ConvenioVigencia[] {
  const resultado: ConvenioVigencia[] = [];
  for (const row of rows) {
    if (!ehConvenio(row) || encerrado(row)) continue;
    const fim = dataFim(row);
    if (!fim) continue;
    const dias = diasEntre(fim, hoje);
    if (dias < 0 || dias > janela) continue;
    const repasse = toNumber(row.valor_repasse);
    const liberado = toNumber(row.valor_desembolsado);
    resultado.push({
      row,
      fim,
      dias,
      repasse,
      liberado,
      execucao: repasse > 0 ? liberado / repasse : null,
    });
  }
  return resultado.sort((a, b) => a.dias - b.dias);
}

export type JanelaVigencia = { dias: number; quantidade: number; repasse: number };

export function resumoVigencia(proximas: ConvenioVigencia[]): JanelaVigencia[] {
  return JANELAS_VIGENCIA.map((dias) => {
    const dentro = proximas.filter((item) => item.dias <= dias);
    return {
      dias,
      quantidade: dentro.length,
      repasse: dentro.reduce((total, item) => total + item.repasse, 0),
    };
  });
}

/** Convênios com vigência próxima do fim e execução abaixo do limite. */
export function execucaoBaixa(proximas: ConvenioVigencia[]): ConvenioVigencia[] {
  return proximas.filter((item) => item.execucao !== null && item.execucao < LIMITE_EXECUCAO_BAIXA);
}

/**
 * Série por ano de assinatura, com a situação do convênio empilhada.
 * Mantém as situações mais frequentes e agrupa o restante em "Outros".
 */
export function serieSituacaoPorAno(
  rows: TransferRecord[],
  topN = 6,
): { rows: CompositionRow[]; series: CompositionSeries[] } {
  const base = rows.filter((row) => {
    const ano = String(row.ano_assinatura ?? '').trim();
    const situacao = String(row.situacao_convenio ?? '').trim();
    return ehConvenio(row) && /^\d{4}$/.test(ano) && situacao && !situacao.startsWith('Não se aplica');
  });

  const contagem = new Map<string, number>();
  for (const row of base) {
    const situacao = String(row.situacao_convenio).trim();
    contagem.set(situacao, (contagem.get(situacao) ?? 0) + 1);
  }
  const principais = [...contagem.entries()].sort((a, b) => b[1] - a[1]).slice(0, topN).map(([s]) => s);
  const chaveDe = (situacao: string) => (principais.includes(situacao) ? `s_${principais.indexOf(situacao)}` : 's_outros');

  const series: CompositionSeries[] = principais.map((situacao, index) => ({ key: `s_${index}`, label: situacao }));
  if (contagem.size > principais.length) series.push({ key: 's_outros', label: 'Outros' });

  const porAno = new Map<string, CompositionRow>();
  for (const row of base) {
    const ano = String(row.ano_assinatura).trim();
    const linha = porAno.get(ano) ?? ({ label: ano, total: 0, ...Object.fromEntries(series.map((s) => [s.key, 0])) } as CompositionRow);
    const chave = chaveDe(String(row.situacao_convenio).trim());
    linha[chave] = (linha[chave] as number) + 1;
    linha.total += 1;
    porAno.set(ano, linha);
  }

  return { rows: [...porAno.values()].sort((a, b) => a.label.localeCompare(b.label)), series };
}
