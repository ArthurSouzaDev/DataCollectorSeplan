import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import { formatNumber, type CompositionRow, type CompositionSeries } from '../../services/data/transferData';
import { axisProps, seriesColor } from './chartTheme';

type SituacaoPorAnoChartProps = {
  rows: CompositionRow[];
  series: CompositionSeries[];
};

/**
 * Convênios por ano de assinatura, empilhados por situação. Mostra contagens
 * (não valores), por isso não usa o gráfico de composição, que formata moeda.
 */
export function SituacaoPorAnoChart({ rows, series }: SituacaoPorAnoChartProps) {
  if (rows.length === 0) {
    return <p className="chart-empty">Nenhum convênio com ano de assinatura para os filtros aplicados.</p>;
  }

  return (
    <ResponsiveContainer width="100%" height={340}>
      <BarChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid vertical={false} stroke="var(--border)" />
        <XAxis dataKey="label" {...axisProps} />
        <YAxis {...axisProps} allowDecimals={false} tickFormatter={(v: number) => formatNumber(v)} />
        <Tooltip formatter={(value) => formatNumber(Number(value))} />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        {series.map((serie, index) => (
          <Bar key={serie.key} dataKey={serie.key} name={serie.label} stackId="situacao" fill={seriesColor(index)} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}
