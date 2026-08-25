import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import {
  formatCurrency,
  type CompositionConfig,
  type CompositionRow,
} from '../../services/data/transferData';
import { formatPercent } from '../../services/data/datasetKpis';
import {
  axisProps,
  chartColors,
  formatCompactCurrency,
  MAX_BAR_SIZE,
  seriesColor,
  SURFACE_GAP,
  truncateLabel,
} from './chartTheme';

type CompositionChartProps = {
  rows: CompositionRow[];
  config: CompositionConfig;
  height?: number;
};

/**
 * Barra empilhada parte-todo: cada barra é o valor de um grupo, dividido nas
 * parcelas que o compõem.
 *
 * Horizontal porque os rótulos de grupo (órgão, ano) ficam legíveis à esquerda
 * sem rotação. As parcelas se separam por uma folga de 2px na cor da superfície
 * — a mesma folga em toda a pilha.
 */
export function CompositionChart({ rows, config, height = 300 }: CompositionChartProps) {
  if (!rows.length) {
    return <p className="chart-empty">Sem dados para os filtros aplicados.</p>;
  }

  return (
    <div className="composition-chart">
      <ul className="chart-legend">
        {config.series.map((serie, index) => (
          <li key={serie.key}>
            <i aria-hidden="true" style={{ background: seriesColor(index) }} />
            <span>{serie.label}</span>
          </li>
        ))}
      </ul>

      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 16, bottom: 4, left: 8 }}>
          <CartesianGrid horizontal={false} stroke={chartColors.border} strokeWidth={1} />
          <XAxis type="number" {...axisProps} tickFormatter={formatCompactCurrency} />
          <YAxis
            type="category"
            dataKey="label"
            width={104}
            {...axisProps}
            tickFormatter={(label: string) => truncateLabel(label, 14)}
          />
          <Tooltip
            cursor={{ fill: 'var(--surface-hover)', opacity: 0.5 }}
            content={<CompositionTooltip config={config} />}
          />
          {config.series.map((serie, index) => (
            <Bar
              key={serie.key}
              dataKey={serie.key}
              name={serie.label}
              stackId="composicao"
              fill={seriesColor(index)}
              stroke={chartColors.surface}
              strokeWidth={SURFACE_GAP}
              maxBarSize={MAX_BAR_SIZE}
              isAnimationActive
              animationDuration={650}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function CompositionTooltip({
  active,
  payload,
  config,
}: {
  active?: boolean;
  payload?: Array<{ payload?: CompositionRow }>;
  config: CompositionConfig;
}) {
  const row = payload?.[0]?.payload;
  if (!active || !row) return null;

  return (
    <div className="chart-tooltip" role="tooltip">
      <strong>{row.label}</strong>
      {config.series.map((serie, index) => {
        const value = Number(row[serie.key] ?? 0);
        return (
          <span key={serie.key} className="tooltip-row">
            <i aria-hidden="true" style={{ background: seriesColor(index) }} />
            {serie.label}: <b>{formatCurrency(value)}</b>
            {row.total > 0 ? <em>{formatPercent((value / row.total) * 100)}</em> : null}
          </span>
        );
      })}
      <span className="tooltip-total">
        Total: <b>{formatCurrency(row.total)}</b>
      </span>
    </div>
  );
}
