import { serieEmpilhadaPorAno } from './seriePorAno';
import {
  aggregateBy,
  aggregateShare,
  type AggregateRow,
  type CompositionRow,
  type CompositionSeries,
  type DatasetId,
  type ShareRow,
  type TransferRecord,
} from './transferData';

/**
 * Gráficos complementares de especiais e fundo a fundo.
 * Discricionárias tem os seus próprios indicadores (convenioInsights), então não entra aqui.
 */
export type Destaques = {
  /** `coluna` é o campo que o clique no gráfico filtra. */
  ranking: { titulo: string; dica: string; coluna: string; linhas: AggregateRow[] };
  /** Clicar numa faixa filtra pela situação (coluna `situacao`). */
  serie: { titulo: string; dica: string; rows: CompositionRow[]; series: CompositionSeries[] };
  participacao: { titulo: string; dica: string; coluna: string; slices: ShareRow[]; total: number };
};

export function destaquesDe(id: DatasetId, rows: TransferRecord[]): Destaques | null {
  if (id === 'especiais') {
    return {
      ranking: {
        titulo: 'Ranking por beneficiário',
        dica: 'Valor total · 10 maiores',
        coluna: 'beneficiario',
        linhas: aggregateBy(rows, 'beneficiario', 'valor_total', 10),
      },
      serie: {
        titulo: 'Situação por ano da emenda',
        dica: 'Ano da emenda · quantidade de planos',
        ...serieEmpilhadaPorAno(rows, 'ano_emenda', 'situacao'),
      },
      participacao: {
        titulo: 'Participação por parlamentar',
        dica: 'Quantidade de planos',
        coluna: 'parlamentar',
        ...aggregateShare(rows, 'parlamentar', 'valor_total'),
      },
    };
  }

  if (id === 'fundo-a-fundo') {
    return {
      ranking: {
        titulo: 'Ranking por município',
        dica: 'Total repassado · 10 maiores',
        coluna: 'municipio',
        linhas: aggregateBy(rows, 'municipio', 'valor_total_repasse', 10),
      },
      serie: {
        titulo: 'Situação por ano',
        dica: 'Ano de início do plano · quantidade de planos',
        ...serieEmpilhadaPorAno(rows, 'ano', 'situacao'),
      },
      participacao: {
        titulo: 'Participação por fundo repassador',
        dica: 'Quantidade de planos',
        coluna: 'fundo_repassador',
        ...aggregateShare(rows, 'fundo_repassador', 'valor_total_repasse'),
      },
    };
  }

  return null;
}
