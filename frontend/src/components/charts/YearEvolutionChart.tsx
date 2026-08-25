import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { formatCurrency, formatNumber, type AggregateRow } from '../../services/data/transferData';
import { axisProps, chartColors, formatCompactAmount, MAX_BAR_SIZE } from './chartTheme';

type YearEvolutionChartProps = {
  rows: AggregateRow[];
  valueLabel?: string;
  countLabel?: string;
};

/**
 * Evolução por ano — valor e quantidade.
 *
 * O dashboard Streamlit desenhava as duas medidas num gráfico só, com eixo Y
 * secundário. Dois eixos com escalas diferentes no mesmo plot fabricam uma
 * correlação que não está no dado (o alinhamento entre as escalas é arbitrário),
 * então aqui viram dois gráficos empilhados que compartilham o eixo X: a leitura
 * de "subiu junto" continua possível, mas agora é honesta.
 */
export function YearEvolutionChart({
  rows,
  valueLabel = 'Valor',
  countLabel = 'Registros',
}: YearEvolutionChartProps) {
  if (!rows.length) {
    return <p className="chart-empty">Sem dados para os filtros aplicados.</p>;
  }

  // Margens idênticas nos dois gráficos: é o que mantém as colunas e os pontos
  // alinhados na mesma posição de X.
  const margin = { top: 8, right: 12, bottom: 0, left: 4 };
  const axisWidth = 58;

  return (
    <div className="year-evolution">
      <div className="year-evolution-plot">
        <span className="chart-caption">{valueLabel}</span>
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={rows} margin={margin}>
            <CartesianGrid vertical={false} stroke={chartColors.border} strokeWidth={1} />
            <XAxis dataKey="label" {...axisProps} tick={false} height={2} />
            <YAxis width={axisWidth} {...axisProps} tickFormatter={formatCompactAmount} />
            <Tooltip
              cursor={{ fill: 'var(--surface-hover)', opacity: 0.5 }}
              content={<YearTooltip valueLabel={valueLabel} countLabel={countLabel} />}
            />
            <Bar
              dataKey="value"
              fill={chartColors.accent}
              radius={[4, 4, 0, 0]}
              maxBarSize={MAX_BAR_SIZE}
              isAnimationActive
              animationDuration={650}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="year-evolution-plot">
        <span className="chart-caption">{countLabel}</span>
        <ResponsiveContainer width="100%" height={120}>
          <LineChart data={rows} margin={{ ...margin, bottom: 4 }}>
            <CartesianGrid vertical={false} stroke={chartColors.border} strokeWidth={1} />
            <XAxis dataKey="label" {...axisProps} />
            <YAxis width={axisWidth} {...axisProps} tickFormatter={(value: number) => formatNumber(value)} />
            <Tooltip
              cursor={{ stroke: chartColors.border, strokeWidth: 1 }}
              content={<YearTooltip valueLabel={valueLabel} countLabel={countLabel} />}
            />
            <Line
              type="monotone"
              dataKey="count"
              stroke={chartColors.warm}
              strokeWidth={2}
              strokeLinecap="round"
              strokeLinejoin="round"
              dot={{ r: 4, fill: chartColors.warm, stroke: chartColors.surface, strokeWidth: 2 }}
              activeDot={{ r: 5, stroke: chartColors.surface, strokeWidth: 2 }}
              isAnimationActive
              animationDuration={650}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

/** Um tooltip só, com as duas medidas daquele ano — em qualquer um dos plots. */
function YearTooltip({
  active,
  payload,
  label,
  valueLabel,
  countLabel,
}: {
  active?: boolean;
  payload?: Array<{ payload?: AggregateRow }>;
  label?: string | number;
  valueLabel: string;
  countLabel: string;
}) {
  const row = payload?.[0]?.payload;
  if (!active || !row) return null;

  return (
    <div className="chart-tooltip" role="tooltip">
      <strong>{label !== undefined ? String(label) : row.label}</strong>
      <span>
        {valueLabel}: <b>{formatCurrency(row.value)}</b>
      </span>
      <span>
        {countLabel}: <b>{formatNumber(row.count)}</b>
      </span>
    </div>
  );
}
