import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';

import { formatNumber, type ShareRow } from '../../services/data/transferData';
import { formatPercent } from '../../services/data/datasetKpis';
import { chartColors, seriesColor, SURFACE_GAP } from './chartTheme';

type CategoryDonutProps = {
  slices: ShareRow[];
  total: number;
  /** Rótulos selecionados no filtro correspondente — ficam destacados. */
  selected?: string[];
  onSelect?: (label: string) => void;
  totalLabel?: string;
};

/**
 * Rosca de distribuição (parte-todo) com no máximo 6 fatias.
 *
 * A identidade nunca fica só na cor: a legenda repete rótulo, quantidade e
 * percentual em tokens de texto, e a fatia "Outros" agrega a cauda.
 */
export function CategoryDonut({
  slices,
  total,
  selected = [],
  onSelect,
  totalLabel = 'registros',
}: CategoryDonutProps) {
  if (!slices.length) {
    return <p className="chart-empty">Sem dados para os filtros aplicados.</p>;
  }

  const selectedSet = new Set(selected);
  const hasSelection = selectedSet.size > 0;
  // "Outros" agrega várias categorias, então não vira filtro.
  const interactive = typeof onSelect === 'function';

  return (
    <div className="donut-chart">
      <div className="donut-plot">
        <ResponsiveContainer width="100%" height={210}>
          <PieChart>
            <Tooltip content={<DonutTooltip />} />
            <Pie
              data={slices}
              dataKey="count"
              nameKey="label"
              innerRadius="58%"
              outerRadius="92%"
              /* O separador de 2px é a superfície aparecendo entre as fatias,
                 não um contorno desenhado em volta da marca. */
              stroke={chartColors.surface}
              strokeWidth={SURFACE_GAP}
              isAnimationActive
              animationDuration={650}
            >
              {slices.map((slice, index) => (
                <Cell
                  key={slice.label}
                  fill={slice.label === 'Outros' ? chartColors.mutedLight : seriesColor(index)}
                  fillOpacity={hasSelection && !selectedSet.has(slice.label) ? 0.4 : 1}
                  cursor={interactive && slice.label !== 'Outros' ? 'pointer' : 'default'}
                  onClick={
                    interactive && slice.label !== 'Outros' ? () => onSelect?.(slice.label) : undefined
                  }
                />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        <div className="donut-center">
          <strong>{formatNumber(total)}</strong>
          <span>{totalLabel}</span>
        </div>
      </div>

      <ul className="donut-legend">
        {slices.map((slice, index) => {
          const dimmed = hasSelection && !selectedSet.has(slice.label);
          const clickable = interactive && slice.label !== 'Outros';
          return (
            <li key={slice.label} className={dimmed ? 'dimmed' : undefined}>
              <button
                type="button"
                onClick={clickable ? () => onSelect?.(slice.label) : undefined}
                disabled={!clickable}
                title={slice.label}
              >
                <i
                  aria-hidden="true"
                  style={{
                    background: slice.label === 'Outros' ? chartColors.mutedLight : seriesColor(index),
                  }}
                />
                <span>{slice.label}</span>
                <em>{formatPercent(slice.share)}</em>
                <b>{formatNumber(slice.count)}</b>
              </button>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function DonutTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload?: ShareRow }>;
}) {
  const row = payload?.[0]?.payload;
  if (!active || !row) return null;

  return (
    <div className="chart-tooltip" role="tooltip">
      <strong>{row.label}</strong>
      <span>
        Registros: <b>{formatNumber(row.count)}</b>
      </span>
      <span>Participação: {formatPercent(row.share)}</span>
    </div>
  );
}
