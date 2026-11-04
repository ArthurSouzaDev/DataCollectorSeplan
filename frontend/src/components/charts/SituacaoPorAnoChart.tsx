import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import { formatNumber, type CompositionRow, type CompositionSeries } from '../../services/data/transferData';
import { axisProps, seriesColor } from './chartTheme';
import { LegendaRecolhivel } from './LegendaRecolhivel';

type SituacaoPorAnoChartProps = {
  rows: CompositionRow[];
  series: CompositionSeries[];
  /** Recebe a situação da faixa clicada. "Outros" não filtra, porque agrupa várias situações. */
  onSelect?: (situacao: string) => void;
};

/**
 * Convênios por ano de assinatura, empilhados por situação. Mostra contagens
 * (não valores), por isso não usa o gráfico de composição, que formata moeda.
 */
export function SituacaoPorAnoChart({ rows, series, onSelect }: SituacaoPorAnoChartProps) {
  if (rows.length === 0) {
    return <p className="chart-empty">Nenhum convênio com ano de assinatura para os filtros aplicados.</p>;
  }

  return (
    <div className="situacao-chart">
    <ResponsiveContainer width="100%" height={340}>
      <BarChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid vertical={false} stroke="var(--border)" />
        <XAxis dataKey="label" {...axisProps} />
        <YAxis {...axisProps} allowDecimals={false} tickFormatter={(v: number) => formatNumber(v)} />
        <Tooltip formatter={(value) => formatNumber(Number(value))} />
        {series.map((serie, index) => (
          <Bar
            key={serie.key}
            dataKey={serie.key}
            name={serie.label}
            stackId="situacao"
            fill={seriesColor(index)}
            cursor={onSelect && serie.label !== 'Outros' ? 'pointer' : undefined}
            onClick={() => {
              if (onSelect && serie.label !== 'Outros') onSelect(serie.label);
            }}
          />
        ))}
      </BarChart>
    </ResponsiveContainer>

      {/* Legenda clicável: faixas finas são difíceis de clicar, e a legenda cobre todas as situações. */}
      <LegendaRecolhivel quantidade={series.length}>
      <ul className="donut-legend">
        {series.map((serie, index) => {
          const clicavel = Boolean(onSelect) && serie.label !== 'Outros';
          return (
            <li key={serie.key}>
              <button
                type="button"
                onClick={clicavel ? () => onSelect?.(serie.label) : undefined}
                disabled={!clicavel}
                title={serie.label}
              >
                <i aria-hidden="true" style={{ background: seriesColor(index) }} />
                <span>{serie.label}</span>
              </button>
            </li>
          );
        })}
      </ul>
      </LegendaRecolhivel>
    </div>
  );
}
