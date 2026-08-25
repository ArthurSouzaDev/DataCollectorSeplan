import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import type { AggregateRow } from '../../services/data/transferData';
import { ChartTooltip } from './ChartTooltip';
import { axisProps, chartColors, formatCompactCurrency, MAX_BAR_SIZE, truncateLabel } from './chartTheme';

type RankingBarChartProps = {
  rows: AggregateRow[];
  valueLabel?: string;
  /** Rótulos atualmente selecionados no filtro — ficam destacados. */
  selected?: string[];
  /** Clique na barra: usado para aplicar/remover o filtro do agrupamento. */
  onSelect?: (label: string) => void;
  height?: number;
};

export function RankingBarChart({
  rows,
  valueLabel = 'Valor',
  selected = [],
  onSelect,
  height = 320,
}: RankingBarChartProps) {
  if (!rows.length) {
    return <p className="chart-empty">Sem dados para os filtros aplicados.</p>;
  }

  const selectedSet = new Set(selected);
  const hasSelection = selectedSet.size > 0;
  const interactive = typeof onSelect === 'function';
  // O eixo de categorias cresce de baixo para cima: inverte para o maior ficar no topo.
  const data = rows.slice().reverse();

  return (
    <div className={interactive ? 'chart-frame interactive' : 'chart-frame'}>
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} layout="vertical" margin={{ top: 4, right: 16, bottom: 4, left: 8 }}>
          <CartesianGrid horizontal={false} stroke={chartColors.border} strokeWidth={1} />
          <XAxis type="number" {...axisProps} tickFormatter={formatCompactCurrency} />
          <YAxis
            type="category"
            dataKey="label"
            width={160}
            {...axisProps}
            tickFormatter={(label: string) => truncateLabel(label, 22)}
          />
          <Tooltip
            cursor={{ fill: 'var(--surface-hover)', opacity: 0.5 }}
            content={<ChartTooltip valueLabel={valueLabel} hint={interactive ? 'Clique para filtrar' : undefined} />}
          />
          <Bar
            dataKey="value"
            radius={[0, 4, 4, 0]}
            maxBarSize={MAX_BAR_SIZE}
            onClick={interactive ? (entry: unknown) => onSelect?.((entry as AggregateRow).label) : undefined}
            cursor={interactive ? 'pointer' : undefined}
            isAnimationActive
            animationDuration={650}
          >
            {data.map((row) => (
              <Cell
                key={row.label}
                fill={selectedSet.has(row.label) ? chartColors.accentStrong : chartColors.accent}
                fillOpacity={hasSelection && !selectedSet.has(row.label) ? 0.42 : 1}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
