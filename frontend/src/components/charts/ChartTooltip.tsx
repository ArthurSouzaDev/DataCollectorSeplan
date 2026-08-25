import { formatCurrency, formatNumber } from '../../services/data/transferData';

export type TooltipRow = { label: string; value: number; count?: number };

type ChartTooltipProps = {
  active?: boolean;
  payload?: Array<{ payload?: TooltipRow; value?: number; name?: string; color?: string }>;
  label?: string | number;
  valueLabel?: string;
  hint?: string;
};

/**
 * Tooltip único para todos os gráficos: formata em R$ pt-BR e mostra a
 * contagem de registros quando o dado agregado a traz.
 */
export function ChartTooltip({ active, payload, label, valueLabel = 'Valor', hint }: ChartTooltipProps) {
  if (!active || !payload?.length) return null;

  const entry = payload[0];
  const row = entry?.payload;
  const value = typeof entry?.value === 'number' ? entry.value : (row?.value ?? 0);
  const title = row?.label ?? (label !== undefined ? String(label) : '');

  return (
    <div className="chart-tooltip" role="tooltip">
      {title ? <strong>{title}</strong> : null}
      <span>
        {valueLabel}: <b>{formatCurrency(value)}</b>
      </span>
      {typeof row?.count === 'number' ? <span>Registros: {formatNumber(row.count)}</span> : null}
      {hint ? <em>{hint}</em> : null}
    </div>
  );
}
